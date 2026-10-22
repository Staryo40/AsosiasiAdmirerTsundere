"use strict";

window.addEventListener("load", function () {
  window.ui = SwaggerUIBundle({
    urls: [
      { name: "BNPB Public", url: "/specs/bnpb-public.yaml" },
      { name: "BMKG Source", url: "/specs/bmkg.yaml" },
      { name: "PVMBG Source and Admin", url: "/specs/pvmbg.yaml" },
    ],
    "urls.primaryName": "BNPB Public",
    dom_id: "#swagger-ui",
    deepLinking: true,
    displayRequestDuration: true,
    docExpansion: "list",
    filter: true,
    persistAuthorization: false,
    queryConfigEnabled: false,
    supportedSubmitMethods: ["get", "post"],
    validatorUrl: null,
    presets: [
      SwaggerUIBundle.presets.apis,
      SwaggerUIStandalonePreset,
    ],
    layout: "StandaloneLayout",
  });
});
