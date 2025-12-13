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
Render RUSTFS_VOLUMES
*/}}
{{- define "rustfs.volumes" -}}
{{- if eq (int .Values.replicaCount) 4 }}
{{- printf "http://%s-{0...%d}.%s-headless:%d/data/rustfs{0...%d}" (include "rustfs.fullname" .) (sub (.Values.replicaCount | int) 1) (include "rustfs.fullname" . ) (.Values.service.ep_port | int) (sub (.Values.replicaCount | int) 1) }}
{{- end }}
{{- if eq (int .Values.replicaCount) 16 }}
{{- printf "http://%s-{0...%d}.%s-headless:%d/data" (include "rustfs.fullname" .) (sub (.Values.replicaCount | int) 1) (include "rustfs.fullname" .) (.Values.service.ep_port | int) }}
{{- end }}
{{- end }}

