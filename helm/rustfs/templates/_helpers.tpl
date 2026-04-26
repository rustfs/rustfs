{{/*
Expand the name of the chart.
*/}}
{{- define "rustfs.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "rustfs.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "rustfs.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "rustfs.labels" -}}
helm.sh/chart: {{ include "rustfs.chart" . }}
{{ include "rustfs.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "rustfs.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rustfs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "rustfs.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "rustfs.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Return the secret name
*/}}
{{- define "rustfs.secretName" -}}
{{- if .Values.secret.existingSecret }}
{{- .Values.secret.existingSecret }}
{{- else }}
{{- printf "%s-secret" (include "rustfs.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Return image pull secret content
*/}}
{{- define "imagePullSecret" }}
{{- with .Values.imageRegistryCredentials }}
{{- printf "{\"auths\":{\"%s\":{\"username\":\"%s\",\"password\":\"%s\",\"email\":\"%s\",\"auth\":\"%s\"}}}" .registry .username .password .email (printf "%s:%s" .username .password | b64enc) | b64enc }}
{{- end }}
{{- end }}

{{/*
Return the default imagePullSecret name
*/}}
{{- define "rustfs.imagePullSecret.name" -}}
{{- printf "%s-registry-secret" (include "rustfs.fullname" .) }}
{{- end }}

{{/*
Render imagePullSecrets for workloads - appends registry secret
*/}}
{{- define "chart.imagePullSecrets" -}}
{{- $secrets := .Values.imagePullSecrets | default list }}
{{- if .Values.imageRegistryCredentials.enabled }}
{{- $secrets = append $secrets (dict "name" (include "rustfs.imagePullSecret.name" .)) }}
{{- end }}
{{- toYaml $secrets }}
{{- end }}

{{/*
Render annotations for the main Service resource.
Merges (in order of increasing precedence):
  - service.traefikAnnotations  (when ingress.className=traefik)
  - ingress.traefikAnnotations  (when ingress.className=traefik, backwards-compat alias)
  - service.annotations
*/}}
{{- define "rustfs.serviceAnnotations" -}}
{{- $annotations := dict }}
{{- if and .Values.mode.distributed.enabled (eq .Values.ingress.className "traefik") }}
{{- $annotations = merge $annotations (default (dict) .Values.service.traefikAnnotations) }}
{{- $annotations = merge $annotations (default (dict) .Values.ingress.traefikAnnotations) }}
{{- end }}
{{- $annotations = merge $annotations (default (dict) .Values.service.annotations) }}
{{- if and .Values.mode.distributed.enabled .Values.mtls.enabled (eq .Values.ingress.className "traefik") }}
{{- $mtls := dict
  "traefik.ingress.kubernetes.io/service.serversscheme" "https"
  "traefik.ingress.kubernetes.io/service.serverstransport" (printf "%s-%s-transport@kubernetescrd" .Release.Namespace (include "rustfs.fullname" .))
}}
{{- $annotations = merge $annotations $mtls }}
{{- end }}
{{- if $annotations }}
{{- toYaml $annotations }}
{{- end }}
{{- end }}

{{/*
Render annotations for the headless Service resource.
Merges:
  - service.headlessAnnotations
*/}}
{{- define "rustfs.headlessServiceAnnotations" -}}
{{- $annotations := default (dict) .Values.service.headlessAnnotations }}
{{- if $annotations }}
{{- toYaml $annotations }}
{{- end }}
{{- end }}

{{/*
Render annotations for the Ingress resource.
Merges (in order of increasing precedence):
  - ingress.nginxAnnotations    (when ingress.className=nginx)
  - ingress.traefikAnnotations  (when ingress.className=traefik)
  - ingress.customAnnotations   (backwards-compat)
  - ingress.annotations
*/}}
{{- define "rustfs.ingressAnnotations" -}}
{{- $annotations := dict }}
{{- if eq .Values.ingress.className "nginx" }}
{{- $annotations = merge $annotations (default (dict) .Values.ingress.nginxAnnotations) }}
{{- else if eq .Values.ingress.className "traefik" }}
{{- $annotations = merge $annotations (default (dict) .Values.ingress.traefikAnnotations) }}
{{- end }}
{{- $annotations = merge $annotations (default (dict) .Values.ingress.customAnnotations) }}
{{- $annotations = merge $annotations (default (dict) .Values.ingress.annotations) }}
{{- if $annotations }}
{{- toYaml $annotations }}
{{- end }}
{{- end }}

{{/*
Render RUSTFS_VOLUMES
*/}}
{{- define "rustfs.volumes" -}}

{{- $protocol := "http" -}}
{{- if .Values.mtls.enabled -}}
  {{- $protocol = "https" -}}
{{- end -}}

{{- printf "%s://%s-{0...%d}.%s-headless.%s.svc.cluster.local:%d/data" $protocol (include "rustfs.fullname" .) (sub (.Values.replicaCount | int) 1) (include "rustfs.fullname" .) .Release.Namespace (.Values.service.endpoint.port | int) }}
{{- end }}

{{/*
Render RUSTFS_SERVER_DOMAINS
*/}}

{{- define "rustfs.serverDomains" -}}
{{- $domains := list .Values.config.rustfs.domains -}}
{{- $fullname := include "rustfs.fullname" . -}}
{{- $replicaCount := int .Values.replicaCount -}}
{{- $servicePort := .Values.service.endpoint.port | default 9000 -}}
{{- range $i := until $replicaCount -}}
  {{- $podDomain := printf "%s-%d.%s-headless:%d" $fullname $i $fullname (int $servicePort) -}}
  {{- $domains = append $domains $podDomain -}}
{{- end -}}
{{- join "," $domains -}}
{{- end -}}

{{/* Render probe command for liveness and readiness
*/}}

{{- define "rustfs.probeCommand" -}}
{{- $endpoint_port := .Values.service.endpoint.port | default 9000 -}}
{{- $console_port := .Values.service.console.port | default 9001 -}}
{{- $args := "-skf" -}}

{{- if and .Values.mtls.enabled -}}
  {{- $args = printf "%s --cert %s --key %s" $args .Values.mtls.clientCertPath .Values.mtls.clientKeyPath -}}
{{- end -}}
- /bin/sh
- -c
- |
  curl {{ $args }} https://127.0.0.1:{{ $endpoint_port }}/health/ready && \
  curl {{ $args }} https://127.0.0.1:{{ $console_port }}/rustfs/console/health
{{- end -}}

{{/*
Render liveness and readiness probe for http and https
*/}}

{{- define "rustfs.probes" -}}
{{- if .Values.livenessProbe.enabled }}
livenessProbe:
  {{- if .Values.mtls.enabled }}
  exec:
    command:
{{ include "rustfs.probeCommand" . | nindent 6 }}
  {{- else }}
  httpGet:
    path: /health
    port: {{ .Values.service.endpoint.port | default 9000 }}
    scheme: {{ if .Values.mtls.enabled }}HTTPS{{ else }}HTTP{{ end }}
  {{- end }}
  initialDelaySeconds: {{ .Values.livenessProbe.initialDelaySeconds | default 60 }}
  periodSeconds: {{ .Values.livenessProbe.periodSeconds | default 5 }}
  timeoutSeconds: {{ .Values.livenessProbe.timeoutSeconds | default 3 }}
  successThreshold: {{ .Values.livenessProbe.successThreshold | default 1 }}
  failureThreshold: {{ .Values.livenessProbe.failureThreshold | default 3 }}
{{- end }}

{{- if .Values.readinessProbe.enabled }}
readinessProbe:
  {{- if .Values.mtls.enabled }}
  exec:
    command:
{{ include "rustfs.probeCommand" . | nindent 6 }}
  {{- else }}
  httpGet:
    path: /health/ready
    port: {{ .Values.service.endpoint.port | default 9000 }}
    scheme: {{ if .Values.mtls.enabled }}HTTPS{{ else }}HTTP{{ end }}
  {{- end }}
  initialDelaySeconds: {{ .Values.readinessProbe.initialDelaySeconds | default 60 }}
  periodSeconds: {{ .Values.readinessProbe.periodSeconds | default 5 }}
  timeoutSeconds: {{ .Values.readinessProbe.timeoutSeconds | default 3 }}
  successThreshold: {{ .Values.readinessProbe.successThreshold | default 1 }}
  failureThreshold: {{ .Values.readinessProbe.failureThreshold | default 3 }}
{{- end }}
{{- end -}}
