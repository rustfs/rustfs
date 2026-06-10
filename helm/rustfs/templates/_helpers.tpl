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
Return the fully qualified name of a server pool.
Pool 0 keeps the legacy single-pool name so that existing deployments can be
expanded in place without renaming their StatefulSet, pods or PVCs.
Expects a dict with keys "root" (the chart root context) and "index".
*/}}
{{- define "rustfs.poolFullname" -}}
{{- if eq (int .index) 0 -}}
{{- include "rustfs.fullname" .root -}}
{{- else -}}
{{- printf "%s-pool%d" (include "rustfs.fullname" .root) (int .index) -}}
{{- end -}}
{{- end }}

{{/*
Return the normalized list of server pools as JSON.
With pools disabled this is a single pool built from the top-level
replicaCount/storageclass values, which keeps all rendered output identical
to the previous single-pool chart. With pools enabled, each entry of
pools.list becomes one pool; omitted fields inherit the top-level values.
Pools are strictly append-only: the index determines the StatefulSet name,
so entries must never be removed or reordered.
*/}}
{{- define "rustfs.pools" -}}
{{- $pools := list -}}
{{- if and .Values.pools .Values.pools.enabled -}}
{{- if not .Values.mode.distributed.enabled -}}
{{- fail "pools.enabled requires mode.distributed.enabled=true" -}}
{{- end -}}
{{- if not .Values.pools.list -}}
{{- fail "pools.enabled is true but pools.list is empty" -}}
{{- end -}}
{{- range $i, $p := .Values.pools.list -}}
{{- $p = default (dict) $p -}}
{{- $replicas := int (default $.Values.replicaCount $p.replicaCount) -}}
{{- if and (ne $replicas 4) (ne $replicas 16) -}}
{{- fail (printf "pools.list[%d].replicaCount must be 4 or 16, got %d" $i $replicas) -}}
{{- end -}}
{{- $sc := mergeOverwrite (deepCopy $.Values.storageclass) (default (dict) $p.storageclass) -}}
{{- $pools = append $pools (dict "index" $i "fullname" (include "rustfs.poolFullname" (dict "root" $ "index" $i)) "replicaCount" $replicas "storageclass" $sc) -}}
{{- end -}}
{{- else -}}
{{- $pools = append $pools (dict "index" 0 "fullname" (include "rustfs.fullname" .) "replicaCount" (int .Values.replicaCount) "storageclass" .Values.storageclass) -}}
{{- end -}}
{{- toJson $pools -}}
{{- end }}

{{/*
Render RUSTFS_VOLUMES
One volume expression per server pool, joined with spaces (the server splits
RUSTFS_VOLUMES on spaces, one pool per expression).
*/}}
{{- define "rustfs.volumes" -}}
{{- $protocol := "http" -}}
{{- if .Values.mtls.enabled -}}
  {{- $protocol = "https" -}}
{{- end -}}
{{- $headless := printf "%s-headless" (include "rustfs.fullname" .) -}}
{{- $ns := .Release.Namespace -}}
{{- $port := .Values.service.endpoint.port | int -}}
{{- $exprs := list -}}
{{- range $pool := include "rustfs.pools" . | fromJsonArray -}}
{{- $n := int $pool.replicaCount -}}
{{- if eq $n 4 -}}
{{- $exprs = append $exprs (printf "%s://%s-{0...%d}.%s.%s.svc.cluster.local:%d/data/rustfs{0...%d}" $protocol $pool.fullname (sub $n 1) $headless $ns $port (sub $n 1)) -}}
{{- else if eq $n 16 -}}
{{- $exprs = append $exprs (printf "%s://%s-{0...%d}.%s.%s.svc.cluster.local:%d/data" $protocol $pool.fullname (sub $n 1) $headless $ns $port) -}}
{{- end -}}
{{- end -}}
{{- join " " $exprs -}}
{{- end }}

{{/*
Render RUSTFS_SERVER_DOMAINS
*/}}

{{- define "rustfs.serverDomains" -}}
{{- $domains := list .Values.config.rustfs.domains -}}
{{- $headless := printf "%s-headless" (include "rustfs.fullname" .) -}}
{{- $servicePort := .Values.service.endpoint.port | default 9000 -}}
{{- range $pool := include "rustfs.pools" . | fromJsonArray -}}
{{- range $i := until (int $pool.replicaCount) -}}
  {{- $podDomain := printf "%s-%d.%s:%d" $pool.fullname $i $headless (int $servicePort) -}}
  {{- $domains = append $domains $podDomain -}}
{{- end -}}
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
