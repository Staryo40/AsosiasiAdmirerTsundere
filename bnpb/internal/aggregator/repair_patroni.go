package aggregator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const repairHTTPBodyLimit = 1 << 20

var errRepairMemberUnavailable = errors.New("Patroni repair member is not running")

type repairProbeMode uint8

const (
	repairProbePrimary repairProbeMode = iota
	repairProbeReplica
	repairProbeTarget
)

type patroniObservation struct {
	Identity struct {
		Scope string `json:"scope"`
		Name  string `json:"name"`
	} `json:"patroni"`
	SystemID string         `json:"database_system_identifier"`
	Role     string         `json:"role"`
	State    string         `json:"state"`
	Timeline uint32         `json:"timeline"`
	Tags     map[string]any `json:"tags"`
}

func probeRepairMember(ctx context.Context, shardID int16, member repairMember, primary bool) (repairEvidence, error) {
	if primary {
		return probeRepair(ctx, shardID, member, repairProbePrimary)
	}
	return probeRepair(ctx, shardID, member, repairProbeReplica)
}

func probeRepairTarget(ctx context.Context, shardID int16, member repairMember) (repairEvidence, error) {
	return probeRepair(ctx, shardID, member, repairProbeTarget)
}

func probeRepair(ctx context.Context, shardID int16, member repairMember, mode repairProbeMode) (repairEvidence, error) {
	// AI-assisted fix: a stalled handshake must leave time to confirm replica loss and inspect the source.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	observed, err := readRepairPatroni(ctx, client, member.PatroniURL)
	if err != nil {
		return repairEvidence{}, err
	}
	if err := observed.verify(member, mode); err != nil {
		return repairEvidence{}, err
	}
	if mode == repairProbePrimary {
		if err := patroniPrimary(ctx, client, member.PatroniURL); err != nil {
			return repairEvidence{}, err
		}
	}
	return probeRepairPostgres(ctx, shardID, member, observed)
}

func readRepairPatroni(ctx context.Context, client *http.Client, address string) (patroniObservation, error) {
	var observed patroniObservation
	base, err := url.Parse(address)
	if err != nil || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || (base.Scheme != "http" && base.Scheme != "https") {
		return observed, errRepairUnsafeEvidence
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(address, "/")+"/patroni", nil)
	if err != nil {
		return observed, errRepairUnsafeEvidence
	}
	response, err := client.Do(request)
	if err != nil {
		return observed, err
	}
	defer response.Body.Close()
	decodeErr := json.NewDecoder(io.LimitReader(response.Body, repairHTTPBodyLimit)).Decode(&observed)
	if decodeErr != nil || (response.StatusCode != http.StatusOK &&
		!(response.StatusCode == http.StatusServiceUnavailable && observed.State != "running")) {
		return observed, fmt.Errorf("Patroni observation unavailable (HTTP %d): %v", response.StatusCode, decodeErr)
	}
	return observed, nil
}

func (observed patroniObservation) verify(member repairMember, mode repairProbeMode) error {
	if observed.Identity.Scope != member.Scope || observed.Identity.Name != member.Name {
		return errRepairUnsafeEvidence
	}
	expectedRole := "replica"
	if mode == repairProbePrimary {
		expectedRole = "primary"
	}
	// A bootstrapping member may not have assumed its PostgreSQL role yet.
	if strings.TrimSpace(observed.State) != "" && observed.State != "running" &&
		(observed.Role == expectedRole || observed.Role == "uninitialized" || observed.Role == "") {
		return errRepairMemberUnavailable
	}
	if observed.State != "running" || observed.Role != expectedRole || observed.SystemID == "" || observed.Timeline == 0 ||
		(mode == repairProbeTarget && !patroniNoFailover(observed.Tags)) {
		return errRepairUnsafeEvidence
	}
	return nil
}

func probeRepairPostgres(ctx context.Context, shardID int16, member repairMember, observed patroniObservation) (repairEvidence, error) {
	primary := observed.Role == "primary"
	conn, err := pgx.Connect(ctx, member.DSN)
	if err != nil {
		return repairEvidence{}, err
	}
	defer closeRepairConnection(conn)
	var node, unit, role, systemID, replayLSN string
	var shard int16
	var inRecovery bool
	if err := conn.QueryRow(ctx, `SELECT COALESCE(current_setting('bnpb.node_name',true),''),
        COALESCE(current_setting('bnpb.unit_name',true),''), store_identity.role, store_identity.shard_id,
        pg_is_in_recovery(), (pg_control_system()).system_identifier::text, COALESCE(pg_last_wal_replay_lsn()::text,'')
        FROM store_identity WHERE singleton`).Scan(&node, &unit, &role, &shard, &inRecovery, &systemID, &replayLSN); err != nil {
		return repairEvidence{}, err
	}
	if node != member.Node || unit != member.Unit || role != "shard" || shard != shardID || inRecovery == primary ||
		systemID == "" || systemID != observed.SystemID {
		return repairEvidence{}, errRepairUnsafeEvidence
	}
	result := repairEvidence{Unit: unit, Node: node, Scope: observed.Identity.Scope, Name: observed.Identity.Name, SystemID: systemID,
		ShardID: shard, Timeline: observed.Timeline, ReplayLSN: replayLSN}
	if primary {
		if err := conn.QueryRow(ctx, "SELECT pg_current_wal_flush_lsn()::text").Scan(&result.PrimaryLSN); err != nil {
			return repairEvidence{}, err
		}
	}
	return result, nil
}

func closeRepairConnection(conn *pgx.Conn) {
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = conn.Close(closeCtx)
}

func patroniPrimary(ctx context.Context, client *http.Client, base string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+"/primary", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("Patroni primary endpoint returned HTTP %d", response.StatusCode)
	}
	return nil
}

type standbyRepairExecutor struct {
	kubernetes KubernetesConfig
	http       *http.Client
}

func newStandbyRepairExecutor(cfg KubernetesConfig) (*standbyRepairExecutor, error) {
	transport, err := tlsTransport(cfg.CAFile)
	if err != nil {
		return nil, errRepairConfiguration
	}
	return &standbyRepairExecutor{kubernetes: cfg, http: &http.Client{Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (executor *standbyRepairExecutor) Reconcile(ctx context.Context, candidate standbyRepairCandidate, primary repairEvidence) (repairEvidence, error) {
	if err := executor.scaleUp(ctx, candidate); err != nil {
		return repairEvidence{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var boundary repairEvidence
	for {
		target, probeErr := synchronousRepairTarget(ctx, candidate, primary)
		switch {
		case errors.Is(probeErr, errRepairUnsafeEvidence):
			return repairEvidence{}, probeErr
		case probeErr == nil && boundary.PrimaryLSN == "":
			// Pin a flush point only after observing the target streaming synchronously.
			captured, err := verifiedRepairSource(ctx, candidate, primary)
			if err != nil {
				return repairEvidence{}, err
			}
			boundary = captured
		case probeErr == nil && lsnAtLeast(target.ReplayLSN, boundary.PrimaryLSN):
			if _, err := verifiedRepairSource(ctx, candidate, boundary); err != nil {
				return repairEvidence{}, err
			}
			target.PrimaryUnit, target.PrimaryNode, target.PrimaryTimeline = boundary.Unit, boundary.Node, boundary.Timeline
			target.PrimaryLSN, target.Synchronous = boundary.PrimaryLSN, true
			return target, nil
		}
		select {
		case <-ctx.Done():
			return repairEvidence{}, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func synchronousRepairTarget(ctx context.Context, candidate standbyRepairCandidate, primary repairEvidence) (repairEvidence, error) {
	target, err := probeRepairTarget(ctx, candidate.ShardID, candidate.Target)
	if err != nil {
		return repairEvidence{}, err
	}
	if target.SystemID != primary.SystemID || target.Timeline != primary.Timeline {
		return repairEvidence{}, errRepairUnsafeEvidence
	}
	synchronous, err := replicationObservation(ctx, candidate.Source.DSN, candidate.Target.Name)
	if err != nil {
		return repairEvidence{}, err
	}
	if !synchronous {
		return repairEvidence{}, errRepairMemberUnavailable
	}
	return target, nil
}

func verifiedRepairSource(ctx context.Context, candidate standbyRepairCandidate, prior repairEvidence) (repairEvidence, error) {
	source, err := probeRepairMember(ctx, candidate.ShardID, candidate.Source, true)
	if err != nil {
		return repairEvidence{}, err
	}
	if source.Unit != prior.Unit || source.Node != prior.Node || source.Scope != prior.Scope ||
		source.Name != prior.Name || source.SystemID != prior.SystemID || source.Timeline != prior.Timeline ||
		!lsnAtLeast(source.PrimaryLSN, prior.PrimaryLSN) {
		return repairEvidence{}, errRepairUnsafeEvidence
	}
	return source, nil
}

type repairScaleSnapshot struct {
	resourceVersion string
	replicas        float64
}

func (executor *standbyRepairExecutor) scaleUp(ctx context.Context, candidate standbyRepairCandidate) error {
	if candidate.Namespace != executor.kubernetes.Namespace {
		return errRepairUnsafeEvidence
	}
	verified, err := executor.verifyPredeclared(ctx, candidate)
	if err != nil {
		return err
	}
	scaleURL := executor.kubeURL("apps/v1", candidate.Namespace, "statefulsets", candidate.StatefulSet, "scale")
	scale, err := executor.getObject(ctx, scaleURL)
	if err != nil {
		return err
	}
	scaleMetadata, _ := scale["metadata"].(map[string]any)
	scaleSpec, specExists := scale["spec"].(map[string]any)
	scaleVersion, _ := scaleMetadata["resourceVersion"].(string)
	scaleReplicas, supplied := scaleSpec["replicas"]
	if !supplied {
		// Kubernetes Scale omits zero replicas while keeping the spec object.
		scaleReplicas = float64(0)
	}
	replicas, validReplicas := scaleReplicas.(float64)
	if !specExists || !validReplicas || scaleVersion != verified.resourceVersion || replicas != verified.replicas {
		return fmt.Errorf("StatefulSet scale changed during repair preflight")
	}
	if verified.replicas == 1 {
		return nil
	}
	return executor.patchRepairScale(ctx, scaleURL, verified.resourceVersion)
}

func (executor *standbyRepairExecutor) patchRepairScale(ctx context.Context, scaleURL, statefulSetVersion string) error {
	patch, _ := json.Marshal([]any{
		map[string]any{"op": "test", "path": "/metadata/resourceVersion", "value": statefulSetVersion},
		map[string]any{"op": "add", "path": "/spec/replicas", "value": 1},
	})
	request, err := http.NewRequestWithContext(ctx, http.MethodPatch, scaleURL, bytes.NewReader(patch))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json-patch+json")
	response, err := executor.kubeDo(request)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("Kubernetes StatefulSet scale returned HTTP %d", response.StatusCode)
	}
	return nil
}

func (executor *standbyRepairExecutor) verifyPredeclared(ctx context.Context, candidate standbyRepairCandidate) (repairScaleSnapshot, error) {
	object, err := executor.getObject(ctx, executor.kubeURL("apps/v1", candidate.Namespace, "statefulsets", candidate.StatefulSet, ""))
	if err != nil {
		return repairScaleSnapshot{}, err
	}
	metadata, _ := object["metadata"].(map[string]any)
	version, _ := metadata["resourceVersion"].(string)
	spec, _ := object["spec"].(map[string]any)
	desired, replicasSpecified := spec["replicas"].(float64)
	if version == "" || !replicasSpecified || (desired != 0 && desired != 1) {
		return repairScaleSnapshot{}, errRepairUnsafeEvidence
	}
	// Generated claims override Pod volumes with the same name, bypassing the prebound disk.
	if templates, _ := spec["volumeClaimTemplates"].([]any); len(templates) != 0 {
		return repairScaleSnapshot{}, errRepairUnsafeEvidence
	}
	if err := executor.verifyRepairPod(ctx, spec, candidate); err != nil {
		return repairScaleSnapshot{}, err
	}
	if err := executor.verifyRepairClaim(ctx, candidate); err != nil {
		return repairScaleSnapshot{}, err
	}
	return repairScaleSnapshot{resourceVersion: version, replicas: desired}, nil
}

func (executor *standbyRepairExecutor) verifyRepairPod(ctx context.Context, spec map[string]any, candidate standbyRepairCandidate) error {
	template, _ := spec["template"].(map[string]any)
	podMetadata, _ := template["metadata"].(map[string]any)
	labels, _ := podMetadata["labels"].(map[string]any)
	podSpec, _ := template["spec"].(map[string]any)
	nodeName, _ := podSpec["nodeName"].(string)
	claimVolume, configVolume, bindingsValid := podVolumeBindings(podSpec, candidate.Claim, candidate.PatroniConfigMap)
	container := postgresRepairContainer(podSpec)
	podConfig, envValid := postgresRepairEnv(container)
	dataMount := postgresMountAt(container, podConfig.dataDir)
	if nodeName != candidate.Target.Node || labels["role"] != "store" || !bindingsValid || !envValid ||
		dataMount["name"] != claimVolume || dataMount["readOnly"] == true || !postgresMountsPatroniConfig(container, configVolume) {
		return errRepairUnsafeEvidence
	}
	return executor.verifyPatroniConfig(ctx, candidate, podConfig)
}

func (executor *standbyRepairExecutor) verifyPatroniConfig(ctx context.Context, candidate standbyRepairCandidate, podConfig repairPodConfig) error {
	configMap, err := executor.getObject(ctx, executor.kubeURL("v1", candidate.Namespace, "configmaps", candidate.PatroniConfigMap, ""))
	if err != nil {
		return err
	}
	data, _ := configMap["data"].(map[string]any)
	config, _ := data["patroni.yaml"].(string)
	if configMap["immutable"] != true || !validRepairPatroniConfig(config, candidate.Target, podConfig) {
		return errRepairUnsafeEvidence
	}
	return nil
}

func (executor *standbyRepairExecutor) verifyRepairClaim(ctx context.Context, candidate standbyRepairCandidate) error {
	claim, err := executor.getObject(ctx, executor.kubeURL("v1", candidate.Namespace, "persistentvolumeclaims", candidate.Claim, ""))
	if err != nil {
		return err
	}
	claimMetadata, _ := claim["metadata"].(map[string]any)
	claimUID, _ := claimMetadata["uid"].(string)
	claimStatus, _ := claim["status"].(map[string]any)
	claimSpec, _ := claim["spec"].(map[string]any)
	if claimStatus["phase"] != "Bound" || claimUID == "" {
		return errRepairUnsafeEvidence
	}
	volumeName, _ := claimSpec["volumeName"].(string)
	if volumeName == "" {
		return errRepairUnsafeEvidence
	}
	pv, err := executor.getObject(ctx, executor.kubeURL("v1", "", "persistentvolumes", volumeName, ""))
	if err != nil {
		return err
	}
	pvSpec, _ := pv["spec"].(map[string]any)
	local, _ := pvSpec["local"].(map[string]any)
	claimRef, _ := pvSpec["claimRef"].(map[string]any)
	affinity, _ := pvSpec["nodeAffinity"].(map[string]any)
	required, _ := affinity["required"].(map[string]any)
	terms, _ := required["nodeSelectorTerms"].([]any)
	localPath, _ := local["path"].(string)
	if !path.IsAbs(localPath) || claimRef["namespace"] != candidate.Namespace || claimRef["name"] != candidate.Claim ||
		claimRef["uid"] != claimUID || !affinityMatches(terms, candidate.Target.Node) {
		return errRepairUnsafeEvidence
	}
	return nil
}

func podVolumeBindings(podSpec map[string]any, claimName, configMapName string) (string, string, bool) {
	volumes, _ := podSpec["volumes"].([]any)
	var claimVolume, configVolume string
	for _, raw := range volumes {
		volume, _ := raw.(map[string]any)
		name, _ := volume["name"].(string)
		claim, _ := volume["persistentVolumeClaim"].(map[string]any)
		if claim["claimName"] == claimName {
			if claimVolume != "" || claim["readOnly"] == true {
				return "", "", false
			}
			claimVolume = name
		}
		configMap, _ := volume["configMap"].(map[string]any)
		if configMap["name"] == configMapName {
			if configVolume != "" {
				return "", "", false
			}
			items, _ := configMap["items"].([]any)
			if len(items) != 1 {
				return "", "", false
			}
			item, _ := items[0].(map[string]any)
			if item["key"] != "patroni.yaml" || item["path"] != "patroni.yaml" {
				return "", "", false
			}
			configVolume = name
		}
	}
	return claimVolume, configVolume, claimVolume != "" && configVolume != ""
}

func postgresRepairContainer(podSpec map[string]any) map[string]any {
	containers, _ := podSpec["containers"].([]any)
	for _, raw := range containers {
		container, _ := raw.(map[string]any)
		if container["name"] == "postgres" {
			return container
		}
	}
	return nil
}

func postgresMountsPatroniConfig(container map[string]any, volumeName string) bool {
	mount := postgresMountAt(container, "/etc/patroni")
	return patroniCommandUsesConfig(container) && mount["name"] == volumeName && mount["readOnly"] == true
}

func postgresMountAt(container map[string]any, mountPath string) map[string]any {
	mounts, _ := container["volumeMounts"].([]any)
	var found map[string]any
	for _, raw := range mounts {
		mount, _ := raw.(map[string]any)
		mountedAt, _ := mount["mountPath"].(string)
		cleanPath := path.Clean(mountedAt)
		if strings.HasPrefix(cleanPath, mountPath+"/") {
			return nil // A nested mount can shadow the verified config or data.
		}
		if cleanPath == mountPath {
			if found != nil || mountedAt != mountPath || mount["subPath"] != nil || mount["subPathExpr"] != nil {
				return nil
			}
			found = mount
		}
	}
	return found
}

func patroniCommandUsesConfig(container map[string]any) bool {
	command, _ := container["command"].([]any)
	args, _ := container["args"].([]any)
	if len(command) == 2 && len(args) == 0 {
		return command[0] == "patroni" && command[1] == "/etc/patroni/patroni.yaml"
	}
	return len(command) == 1 && len(args) == 1 && command[0] == "patroni" && args[0] == "/etc/patroni/patroni.yaml"
}

type repairPodConfig struct {
	dataDir, roleLabel string
}

func postgresRepairEnv(container map[string]any) (repairPodConfig, bool) {
	envFrom, _ := container["envFrom"].([]any)
	if len(envFrom) != 0 {
		return repairPodConfig{}, false
	}
	var podConfig repairPodConfig
	env, _ := container["env"].([]any)
	for _, raw := range env {
		entry, _ := raw.(map[string]any)
		envName, _ := entry["name"].(string)
		literal, _ := entry["value"].(string)
		switch envName {
		case "PGDATA":
			if podConfig.dataDir != "" || entry["valueFrom"] != nil || !path.IsAbs(literal) ||
				literal == "/" || path.Clean(literal) != literal || strings.Contains(literal, "$") {
				return repairPodConfig{}, false
			}
			podConfig.dataDir = literal
		case "PATRONI_KUBERNETES_ROLE_LABEL":
			if podConfig.roleLabel != "" || entry["valueFrom"] != nil || !safePatroniRoleLabel(literal) {
				return repairPodConfig{}, false
			}
			podConfig.roleLabel = literal
		case "PATRONI_CONFIGURATION", "PATRONI_DATA_DIR", "PATRONI_POSTGRESQL_DATA_DIR", "PATRONI_SCOPE", "PATRONI_NAME",
			"PATRONI_POSTGRESQL_CREATE_REPLICA_METHODS", "PATRONI_POSTGRESQL_PARAMETERS", "PATRONI_TAGS",
			"PATRONI_KUBERNETES_TMP_ROLE_LABEL", "PATRONI_KUBERNETES_BOOTSTRAP_LABELS":
			return repairPodConfig{}, false
		default:
			if strings.HasPrefix(envName, "PATRONI_TAGS_") {
				return repairPodConfig{}, false
			}
		}
	}
	return podConfig, podConfig.dataDir != ""
}

func patroniNoFailover(tags map[string]any) bool {
	noFailover, ok := tags["nofailover"].(bool)
	return ok && noFailover
}

func validRepairPatroniConfig(config string, member repairMember, podConfig repairPodConfig) bool {
	raw := json.RawMessage(config)
	if validateJSON(raw) != nil {
		return false
	}
	var root map[string]any
	if json.Unmarshal(raw, &root) != nil || root["scope"] != member.Scope || root["name"] != member.Name {
		return false
	}
	tags, _ := root["tags"].(map[string]any)
	if !patroniNoFailover(tags) {
		return false
	}
	kubernetes, _ := root["kubernetes"].(map[string]any)
	postgresql, _ := root["postgresql"].(map[string]any)
	return preservesStoreRole(kubernetes, podConfig.roleLabel) && validRepairPostgresConfig(postgresql, podConfig.dataDir)
}

func preservesStoreRole(kubernetes map[string]any, roleLabel string) bool {
	if roleLabel == "" {
		roleLabel, _ = kubernetes["role_label"].(string)
	}
	bootstrapLabels, _ := kubernetes["bootstrap_labels"].(map[string]any)
	_, overwritesStoreRole := bootstrapLabels["role"]
	// Patroni adds and later removes bootstrap labels on the live Pod.
	return safePatroniRoleLabel(roleLabel) && kubernetes["tmp_role_label"] != "role" && !overwritesStoreRole
}

func validRepairPostgresConfig(postgresql map[string]any, dataDir string) bool {
	if postgresql["data_dir"] != dataDir {
		return false
	}
	parameters, _ := postgresql["parameters"].(map[string]any)
	if override, exists := parameters["data_directory"]; exists && override != dataDir {
		return false
	}
	// Patroni falls back to the legacy singular key when the plural key is absent.
	if _, legacyMethod := postgresql["create_replica_method"]; legacyMethod {
		return false
	}
	methods, specified := postgresql["create_replica_methods"]
	if !specified {
		return true
	}
	methodList, ok := methods.([]any)
	return ok && len(methodList) == 1 && methodList[0] == "basebackup"
}

var (
	patroniLabelName   = regexp.MustCompile(`^[A-Za-z0-9]([-_.A-Za-z0-9]*[A-Za-z0-9])?$`)
	patroniLabelPrefix = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)
)

func safePatroniRoleLabel(label string) bool {
	prefix, name, qualified := strings.Cut(label, "/")
	if !qualified {
		name = prefix
	}
	return label != "role" && len(name) <= 63 && patroniLabelName.MatchString(name) &&
		(!qualified || (len(prefix) <= 253 && patroniLabelPrefix.MatchString(prefix)))
}

func affinityMatches(terms []any, node string) bool {
	if len(terms) != 1 {
		return false
	}
	term, _ := terms[0].(map[string]any)
	fields, _ := term["matchFields"].([]any)
	expressions, _ := term["matchExpressions"].([]any)
	switch {
	case len(fields) == 1 && len(expressions) == 0:
		return exactRepairNodeSelector(fields[0], "metadata.name", node)
	case len(expressions) == 1 && len(fields) == 0:
		return exactRepairNodeSelector(expressions[0], "kubernetes.io/hostname", node)
	default:
		return false
	}
}

func exactRepairNodeSelector(raw any, key, node string) bool {
	condition, _ := raw.(map[string]any)
	values, _ := condition["values"].([]any)
	return condition["key"] == key && condition["operator"] == "In" && len(values) == 1 && values[0] == node
}

func replicationObservation(ctx context.Context, sourceDSN, applicationName string) (bool, error) {
	conn, err := pgx.Connect(ctx, sourceDSN)
	if err != nil {
		return false, err
	}
	defer closeRepairConnection(conn)
	var count int
	var streaming, synchronous bool
	err = conn.QueryRow(ctx, `SELECT count(*),COALESCE(bool_and(state='streaming'),false),
        COALESCE(bool_and(sync_state='sync'),false) FROM pg_stat_replication WHERE application_name=$1`, applicationName).
		Scan(&count, &streaming, &synchronous)
	return count == 1 && streaming && synchronous, err
}

func (executor *standbyRepairExecutor) getObject(ctx context.Context, target string) (map[string]any, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	response, err := executor.kubeDo(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("Kubernetes read returned HTTP %d", response.StatusCode)
	}
	var object map[string]any
	if err := json.NewDecoder(io.LimitReader(response.Body, repairHTTPBodyLimit)).Decode(&object); err != nil {
		return nil, fmt.Errorf("Kubernetes object response unavailable: %w", err)
	}
	return object, nil
}

func (executor *standbyRepairExecutor) kubeDo(request *http.Request) (*http.Response, error) {
	token, err := readBearerToken(executor.kubernetes.TokenFile, "Kubernetes service-account")
	if err != nil {
		return nil, errRepairConfiguration
	}
	request.Header.Set("Authorization", "Bearer "+token)
	return executor.http.Do(request)
}

func (executor *standbyRepairExecutor) kubeURL(apiVersion, namespace, resource, name, subresource string) string {
	path := "/apis/" + apiVersion
	if apiVersion == "v1" {
		path = "/api/v1"
	}
	if namespace != "" {
		path += "/namespaces/" + url.PathEscape(namespace)
	}
	path += "/" + resource + "/" + url.PathEscape(name)
	if subresource != "" {
		path += "/" + subresource
	}
	return strings.TrimRight(executor.kubernetes.ServerURL, "/") + path
}

var _ repairExecutor = (*standbyRepairExecutor)(nil)
