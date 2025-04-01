{{- define "pto_management_service.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "pto_management_service.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "pto_management_service.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}
