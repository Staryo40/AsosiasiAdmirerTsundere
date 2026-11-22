#requires -Version 5.1
# Created with assistance from OpenAI Codex.
$ErrorActionPreference = 'Stop'

function Invoke-Checked {
    $command = $args[0]
    $arguments = @($args | Select-Object -Skip 1)
    & $command @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Command exited with code $LASTEXITCODE."
    }
}

Push-Location (Join-Path $PSScriptRoot '../../bnpb')
$container = $null
$burst = $null
$oldDatabase = $env:BNPB_M2_LOCAL_TEST_DATABASE_URL
$oldPostgresBin = $env:BNPB_TEST_POSTGRES_BIN
try {
    if (-not $env:BNPB_TEST_POSTGRES_BIN) {
        $pgCtl = Get-Command pg_ctl -CommandType Application
        $env:BNPB_TEST_POSTGRES_BIN = Split-Path -Parent $pgCtl.Source
    }

    $password = [guid]::NewGuid().ToString('N')
    $database = 'bnpb_m2_step4_test'
    $dockerArgs = @(
        'run', '--detach', '--rm', '--pull=never', '--publish', '127.0.0.1::5432',
        '--tmpfs', '/var/lib/postgresql',
        '--env', "POSTGRES_PASSWORD=$password",
        '--env', "POSTGRES_DB=$database",
        'postgres:18.6-alpine'
    )
    $container = Invoke-Checked docker @dockerArgs
    $deadline = (Get-Date).AddSeconds(30)
    do {
        docker exec $container pg_isready -q -h 127.0.0.1 -d $database
        if ($LASTEXITCODE -eq 0) { break }
        if ((Get-Date) -ge $deadline) { throw 'Fixture PostgreSQL is not ready.' }
        Start-Sleep -Milliseconds 500
    } while ($true)

    $standby = '/var/lib/postgresql/p1-standby'
    $connection = 'host=127.0.0.1 port=5432 user=postgres application_name=p1_standby'
    Invoke-Checked docker exec --user postgres --env "PGPASSWORD=$password" $container pg_basebackup -D $standby -R -C -S p1_standby -X stream --checkpoint=fast -d $connection
    Invoke-Checked docker exec --user postgres $container pg_ctl -D $standby -l "$standby/server.log" -o '-p 5433' -w start
    Invoke-Checked docker exec --user postgres $container psql -v ON_ERROR_STOP=1 -d $database -c "ALTER SYSTEM SET synchronous_standby_names = 'FIRST 1 (p1_standby)'"
    Invoke-Checked docker exec --user postgres $container psql -v ON_ERROR_STOP=1 -d $database -c 'SELECT pg_reload_conf()' | Out-Null
    $ready = "SELECT EXISTS (SELECT 1 FROM pg_stat_replication WHERE application_name='p1_standby' AND state='streaming' AND sync_state='sync')"
    $deadline = (Get-Date).AddSeconds(30)
    do {
        $replicaReady = Invoke-Checked docker exec --user postgres $container psql -At -d $database -c $ready
        if ($replicaReady -eq 't') { break }
        if ((Get-Date) -ge $deadline) { throw 'Fixture standby is not streaming synchronously.' }
        Start-Sleep -Milliseconds 500
    } while ($true)

    $address = Invoke-Checked docker port $container 5432/tcp
    $port = ($address -split ':')[-1]
    $env:BNPB_M2_LOCAL_TEST_DATABASE_URL = "postgres://postgres:$password@127.0.0.1:$port/$database" + '?sslmode=disable'

    Write-Output "P1 local checks at $([DateTime]::UtcNow.ToString('o'))"
    Invoke-Checked git rev-parse HEAD
    Invoke-Checked go version
    Invoke-Checked docker exec $container postgres --version
    Invoke-Checked (Join-Path $env:BNPB_TEST_POSTGRES_BIN pg_ctl) --version

    $pattern = '^Test(Audit|Analytics|Catalog|GlobalReadIntegration|M2(Migration|PersistentPlacement|Repair|Shard|Warning)|Placement|Repair|PatroniRepair|ReplicatedMutationIntegration|ShardWriterIntegration|VerifyRepair)'
    & go test -count=1 -timeout=5m -v -tags=integration -run $pattern ./internal/aggregator | Tee-Object -Variable checks
    $status = $LASTEXITCODE
    Invoke-Checked go test -count=1 -timeout=2m -v ./internal/catalogload ./internal/placementcompare

    $fixture = './internal/placementcompare/testdata'
    Write-Output 'Placement model on six synthetic catalogue records'
    Invoke-Checked go run ./cmd/placement-compare -input "$fixture/catalog.jsonl" -resources "$fixture/resources.json"

    $burst = [IO.Path]::GetTempFileName()
    $record = Get-Content "$fixture/catalog.jsonl" -TotalCount 1 | ConvertFrom-Json
    $rows = foreach ($index in 1..300) {
        $record.event_id = "p1-regional-burst-$index"
        $record | ConvertTo-Json -Compress
    }
    [IO.File]::WriteAllLines($burst, [string[]]$rows)
    Write-Output 'Placement model on 300 new IDs from the same region'
    $burstResult = Invoke-Checked go run ./cmd/placement-compare -input $burst -resources "$fixture/resources.json"
    $burstResult
    $placement = ($burstResult -join [Environment]::NewLine | ConvertFrom-Json).natural
    if ($placement.fixed_hash.logical_rows -ne 300 -or
        $placement.fixed_hash.shards.logical_rows -contains 0 -or
        $placement.longitude_band.shards[2].logical_rows -ne 300) {
        throw 'Unexpected placement for the synthetic regional burst.'
    }

    if ($status -ne 0) { throw 'The Aggregator P1 tests failed.' }
    if ($checks -match '^\s*--- SKIP:' -or -not ($checks -match '^--- PASS: TestReplicatedMutationIntegration ')) {
        throw 'A selected P1 test was skipped.'
    }
} finally {
    $env:BNPB_M2_LOCAL_TEST_DATABASE_URL = $oldDatabase
    $env:BNPB_TEST_POSTGRES_BIN = $oldPostgresBin
    if ($burst) { Remove-Item -LiteralPath $burst }
    if ($container) { docker stop $container | Out-Null }
    Pop-Location
}
