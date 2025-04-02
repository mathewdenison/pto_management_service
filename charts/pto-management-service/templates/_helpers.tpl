{{- define "pto-management-service.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "pto-management-service.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "pto-management-service.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}
