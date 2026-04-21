# RustFS Helm Mode

RustFS helm chart supports **standalone and distributed mode**. For standalone mode, there is only one pod and one pvc; for distributed mode, there are two styles, 4 pods and 16 pvcs(each pod has 4 pvcs), 16 pods and 16 pvcs(each pod has 1 pvc). You should decide which mode and style suits for your situation. You can specify the parameters `mode` and `replicaCount` to install different mode and style.

- **For standalone mode**: Only one pod and one pvc acts as single node single disk; Specify parameters `mode.standalone.enabled="true",mode.distributed.enabled="false"` to install.
- **For distributed mode**(**default**): Multiple pods and multiple pvcs, acts as multiple nodes multiple disks, there are two styles:
    - 4 pods and each pods has 4 pvcs(**default**)
    - 16 pods and each pods has 1 pvc: Specify parameters `replicaCount` with `--set replicaCount="16"` to install.

**NOTE**: Please make sure which mode suits for you situation and specify the right parameter to install rustfs on kubernetes.

---

# Parameters Overview

| Parameter | Type | Default value | Description |
|-----|------|---------|-------------|
| affinity.nodeAffinity | object | `{}` |  |
| affinity.podAntiAffinity.enabled | bool | `true` |  |
| affinity.podAntiAffinity.topologyKey | string | `"kubernetes.io/hostname"` |  |
| commonLabels | object | `{}` | Labels to add to all deployed objects. |
| config.rustfs.address | string | `":9000"` |  |
| config.rustfs.console_address | string | `":9001"` |  |
| config.rustfs.console_enable | string | `"true"` |  |
| config.rustfs.domains | string | `""` | Enable virtual host mode. |
| config.rustfs.log_level | string | `"info"` |  |
| config.rustfs.obs_environment | string | `"development"` |  |
| config.rustfs.obs_log_directory | string | `"/logs"` |  |
| config.rustfs.region | string | `"us-east-1"` |  |
| config.rustfs.volumes | string | `""` |  |
| config.rustfs.log_rotation.size | int | `"100"` | Default log rotation size mb for rustfs. |
| config.rustfs.log_rotation.time | string | `"hour"` | Default log rotation time for rustfs. |
| config.rustfs.log_rotation.keep_files | int | `"30"` | Default log keep files for rustfs.  |
| config.rustfs.metrics.enabled | bool | `false` | Toggle metrics export. |
| config.rustfs.metrics.endpoint | string | `""` | Dedicated metrics endpoint. |
| config.rustfs.scanner.speed | string | `""` | Scanner speed preset: `fastest`, `fast`, `default`, `slow`, `slowest`. |
| config.rustfs.scanner.start_delay_secs | string | `""` | Override scanner cycle interval in seconds with `RUSTFS_SCANNER_START_DELAY_SECS`. |
| config.rustfs.scanner.idle_mode | string | `""` | Override scanner idle throttling flag (`RUSTFS_SCANNER_IDLE_MODE`). |
| config.rustfs.scanner.cache_save_timeout_secs | string | `""` | Override scanner cache save timeout in seconds with `RUSTFS_SCANNER_CACHE_SAVE_TIMEOUT_SECS` (minimum `1`). |
| config.rustfs.obs_endpoint.enabled | bool | `false` | Whether to send metrics/logs/traces/profilings to remote endpoint, eg, OLTP. |
| config.rustfs.obs_endpoint.base_endpoint | string | `""` | Root OTLP/HTTP endpoint, e.g. http://otel-collector:4318. |
| config.rustfs.obs_endpoint.use_stdout | bool | `false` | Whether to output logs to stdout in addition the OLTP. |
| config.rustfs.obs_endpoint.metrics.enabled | bool | `false` | Whether to send metrics to remote endpoint. |
| config.rustfs.obs_endpoint.metrics.endpoint | string | `""` | Remote endpoint url for metrics. |
| config.rustfs.obs_endpoint.trace.enabled | bool | `false` | Whether to send trace to remote endpoint. |
| config.rustfs.obs_endpoint.trace.endpoint | string | `""` | Remote endpoint url for trace. |
| config.rustfs.obs_endpoint.logs.enabled | bool | `false` | Whether to send logs to remote endpoint. |
| config.rustfs.obs_endpoint.logs.endpoint | string | `""` | Remote endpoint url for logs. |
| config.rustfs.obs_endpoint.profiling.enabled | bool | `false` | Whether to send profiling to remote endpoint. |
| config.rustfs.obs_endpoint.profiling.endpoint | string | `""` | Remote endpoint url for profiling. |
| config.rustfs.kms.enabled | bool | `false`| Whether to enable kms. |
| config.rustfs.kms.type | string | `vault`| The kms type that RustFS supported. |
| config.rustfs.kms.vault.vault_backend | string | `""`| The vault backend, `vault-kv2` or `vault-transit`. |
| config.rustfs.kms.vault.vault_address | string | `""`| The vault address. |
| config.rustfs.kms.vault.vault_token | string | `""`| The vault token. |
| config.rustfs.kms.vault.vault_mount_path | string | `"transit"`| The vault mount path, only works if `vault_backend` equals `vault-transit` . |
| config.rustfs.kms.vault.default_key | string | `"transit"`| The master key id for RustFS. |
| extraEnv | map | `[]` |  Extra environment variables for RustFS container. |
| containerSecurityContext.capabilities.drop[0] | string | `"ALL"` |  |
| containerSecurityContext.readOnlyRootFilesystem | bool | `true` |  |
| containerSecurityContext.runAsNonRoot | bool | `true` |  |
| enableServiceLinks | bool | `false` |  |
| extraManifests | list | `[]` | List of additional k8s manifests. |
| fullnameOverride | string | `""` |  |
| image.rustfs.pullPolicy | string | `"IfNotPresent"` |  |
| image.rustfs.repository | string | `"rustfs/rustfs"` | RustFS docker image repository. |
| image.rustfs.tag | string | `""` | Chart appVersion default if unset. |
| imagePullSecrets | list | `[]` | A List of secrets to pull image from private registry. |
| imageRegistryCredentials.email | string | `""` | The email to pull rustfs image from private registry.  |
| imageRegistryCredentials.enabled | bool | `false` | To indicate whether pull image from private registry.  |
| imageRegistryCredentials.password | string | `""` | The password to pull rustfs image from private registry.  |
| imageRegistryCredentials.registry | string | `""` | Private registry url to pull rustfs image. |
| imageRegistryCredentials.username | string | `""` | The username to pull rustfs image from private registry. |
| ingress.className | string | `"nginx"` | Specify the ingress class, traefik or nginx. |
| ingress.enabled | bool | `true` |  |
| ingress.hosts[0].host | string | `"example.rustfs.com"` |  |
| ingress.hosts[0].paths[0].path | string | `"/"` |  |
| ingress.hosts[0].paths[0].pathType | string | `"ImplementationSpecific"` |  |
| ingress.nginxAnnotations."nginx.ingress.kubernetes.io/affinity" | string | `"cookie"` |  |
| ingress.nginxAnnotations."nginx.ingress.kubernetes.io/session-cookie-expires" | string | `"3600"` |  |
| ingress.nginxAnnotations."nginx.ingress.kubernetes.io/session-cookie-hash" | string | `"sha1"` |  |
| ingress.nginxAnnotations."nginx.ingress.kubernetes.io/session-cookie-max-age" | string | `"3600"` |  |
| ingress.nginxAnnotations."nginx.ingress.kubernetes.io/session-cookie-name" | string | `"rustfs"` |  |
| ingress.customAnnotations | dict | `{}` | Additional custom annotations, merged with class-specific stickiness annotations. |
| ingress.traefikAnnotations."traefik.ingress.kubernetes.io/service.sticky.cookie" | string | `"true"` |  |
| ingress.traefikAnnotations."traefik.ingress.kubernetes.io/service.sticky.cookie.httponly" | string | `"true"` |  |
| ingress.traefikAnnotations."traefik.ingress.kubernetes.io/service.sticky.cookie.name" | string | `"rustfs"` |  |
| ingress.traefikAnnotations."traefik.ingress.kubernetes.io/service.sticky.cookie.samesite" | string | `"none"` |  |
| ingress.traefikAnnotations."traefik.ingress.kubernetes.io/service.sticky.cookie.secure" | string | `"true"` |  |
| ingress.tls.enabled | bool | `false` | Enable tls and access rustfs via https. |
| ingress.tls.certManager.enabled | string | `false` | Enable cert manager support to generate certificate automatically. |
| ingress.tls.crt | string | "" | The content of certificate file. |
| ingress.tls.key | string | "" | The content of key file. |
| livenessProbe.failureThreshold | int | `3` |  |
| livenessProbe.httpGet.path | string | `"/health"` |  |
| livenessProbe.httpGet.port | string | `"endpoint"` |  |
| livenessProbe.initialDelaySeconds | int | `10` |  |
| livenessProbe.periodSeconds | int | `5` |  |
| livenessProbe.successThreshold | int | `1` |  |
| livenessProbe.timeoutSeconds | int | `3` |  |
| mode.distributed.enabled | bool | `true` | RustFS distributed mode support, namely multiple pod multiple pvc. |
| mode.standalone.enabled | bool | `false` | RustFS standalone mode support, namely one pod one pvc.  |
| mode.standalone.existingClaim.dataClaim |string |`""` |Whether to use existing pvc claim for data storage. |
| mode.standalone.existingClaim.logsClaim |string |`""` |Whether to use existing pvc claim for logs storage. |
| mtls.enabled | bool | `false` | Enable mtls betweens pods. |
| mtls.clientCertPath | string | `/opt/tls/client_cert.pem` | The path for client cert. |
| mtls.clientKeyPath | string | `/opt/tls/client_key.pem` | The path for client key. |
| mtls.existingIssuerRef.enabled | bool | `false` | Enable to use external/existing certificate issuer.|
| mtls.existingIssuerRef.name | string | `""` | The name of external/existing certificate issuer. |
| mtls.existingIssuerRef.kind | string | `""` | The kind of external/existing certificate iss
uer. `ClusterIssuer` or `Issuer`. |
| mtls.existingIssuerRef.group | string | `""` | The group of external/existing certificate issuer. |
| nameOverride | string | `""` |  |
| nodeSelector | object | `{}` |  |
| pdb.create | bool | `false` | Enable/disable a Pod Disruption Budget creation |
| pdb.maxUnavailable | string | `1` |  |
| pdb.minAvailable | string | `""` |  |
| podAnnotations | object | `{}` |  |
| podLabels | object | `{}` |  |
| podSecurityContext.fsGroup | int | `10001` |  |
| podSecurityContext.runAsGroup | int | `10001` |  |
| podSecurityContext.runAsUser | int | `10001` |  |
| readinessProbe.failureThreshold | int | `3` |  |
| readinessProbe.httpGet.path | string | `"/health/ready"` |  |
| readinessProbe.httpGet.port | string | `"endpoint"` |  |
| readinessProbe.initialDelaySeconds | int | `30` |  |
| readinessProbe.periodSeconds | int | `5` |  |
| readinessProbe.successThreshold | int | `1` |  |
| readinessProbe.timeoutSeconds | int | `3` |  |
| replicaCount | int | `4` | Number of cluster nodes. |
| resources.limits.cpu | string | `"200m"` |  |
| resources.limits.memory | string | `"512Mi"` |  |
| resources.requests.cpu | string | `"100m"` |  |
| resources.requests.memory | string | `"128Mi"` |  |
| secret.existingSecret | string | `""` | Use existing secret with a credentials. |
| secret.rustfs.access_key | string | `"rustfsadmin"` | RustFS Access Key ID |
| secret.rustfs.secret_key | string | `"rustfsadmin"` | RustFS Secret Key ID |
| service.type | string | `"ClusterIP"` |  |
| service.console.nodePort | int | `32001` |  |
| service.console.port | int | `9001` |  |
| service.endpoint.nodePort | int | `32000` |  |
| service.endpoint.port | int | `9000` |  |
| serviceAccount.annotations | object | `{}` |  |
| serviceAccount.automount | bool | `true` |  |
| serviceAccount.create | bool | `true` |  |
| serviceAccount.name | string | `""` |  |
| storageclass.dataStorageSize | string | `"256Mi"` | The storage size for data PVC. |
| storageclass.logStorageSize | string | `"256Mi"` | The storage size for logs PVC. |
| storageclass.name | string | `"local-path"` | The name for StorageClass. |
| storageclass.pvcAnnotations.data | map | `{}` | Data pvc customized annotations. |
| storageclass.pvcAnnotations.logs | map | `{}` | Logs pvc customized annotations. |
| tolerations | list | `[]` |  |
| gatewayApi.enabled | bool | `false` | To enable/disable gateway api support. |
| gatewayApi.gatewayClass | string | `traefik` | Gateway class implementation. |
| gatewayApi.listeners.http.name | string | `web` | Gateway API http listener name. |
| gatewayApi.listeners.http.port| int | `8000` | Gateway API http listener port. |
| gatewayApi.listeners.https.name | string | `websecure` | Gateway API https listener name. |
| gatewayApi.listeners.https.port| int | `8443` | Gateway API https listener port. |
| gatewayApi.hostname | string | Hostname to access RustFS via gateway api. |
| gatewayApi.secretName | string | Secret tls to via RustFS using HTTPS. |
| gatewayApi.existingGateway.name | string | `""` |  The existing gateway name, instead of creating a new one. |
| gatewayApi.existingGateway.namespace | string | `""` |  The namespace of the existing gateway, if not the local namespace. |

---

**NOTE**:

The chart pulls the rustfs image from Docker Hub by default. For private registries, provide either:

- **Existing secrets**: Set `imagePullSecrets` with an array of secret names
  ```yaml
  imagePullSecrets:
    - name: my-existing-secret
  ```

- **Auto-generated secret**: Enable `imageRegistryCredentials.enabled: true` and specify credentials plus your image details
  ```yaml
  imageRegistryCredentials:
    enabled: true
    registry: myregistry.com
    username: myuser
    password: mypass
    email: user@example.com
  ```

Both approaches support pulling from private registries seamlessly and you can also combine them.

- The chart default pull rustfs image from dockerhub, if your rustfs image stores in private registry, you can use either existing image Pull secrets with parameter `imagePullSecrets` or create one setting `imageRegistryCredentials.enabled` to `true`,and then specify the `imageRegistryCredentials.registry/username/password/email` as well as `image.rustfs.repository`,`image.rustfs.tag` to pull rustfs image from your private registry.

- The default storageclass is [`local-path`](https://github.com/rancher/local-path-provisioner),if you want to specify your own storageclass, try to set parameter `storageclass.name`.

- The default size for data and logs dir is **256Mi** which must satisfy the production usage,you should specify `storageclass.dataStorageSize` and `storageclass.logStorageSize` to change the size, for example, 1Ti for data and  1Gi for logs.

# Installation

## Requirement

* Helm V3
* RustFS >= 1.0.0-alpha.69

Due to the traefik and ingress has different session sticky/affinity annotations, and rustfs support both those two controller, you should specify parameter `ingress.className` to select the right one which suits for you.

## Installation with traefik controller

If your ingress class is `traefik`, running the command:

```
helm install rustfs -n rustfs --create-namespace ./ --set ingress.className="traefik"
```

## Installation with nginx controller

If your ingress class is `nginx`, running the command:

```
helm install rustfs -n rustfs --create-namespace ./ --set ingress.className="nginx"
```

# Installation check and rustfs login

Check the pod status

```
kubectl -n rustfs get pods -w
NAME       READY   STATUS    RESTARTS        AGE
rustfs-0   1/1     Running   0               2m27s
rustfs-1   1/1     Running   0               2m27s
rustfs-2   1/1     Running   0               2m27s
rustfs-3   1/1     Running   0               2m27s
```

Check the ingress status

```
kubectl -n rustfs get ing
NAME     CLASS   HOSTS            ADDRESS         PORTS     AGE
rustfs   nginx   example.rustfs.com   10.43.237.152   80, 443   29m
```

Access the rustfs cluster via `https://example.rustfs.com` with the default username and password `rustfsadmin`.

> Replace the `example.rustfs.com` with your own domain as well as the certificates.

# TLS configuration

By default, tls is not enabled. If you want to enable tls(recommendated),you can follow below steps:

* Step 1: Certification generation

You can request cert and key from CA or use the self-signed cert(**not recommendated on prod**), and put those two files(eg, `tls.crt` and `tls.key`) under some directory on server, for example `tls` directory.

* Step 2: Certification specifying

You should use `--set-file` parameter when running `helm install` command, for example, running the below command can enable ingress tls and generate tls secret:

```
helm install rustfs rustfs/rustfs -n rustfs --set tls.enabled=true,--set-file tls.crt=./tls.crt,--set-file tls.key=./tls.key
```

# Gateway API support (alpha)

Due to [ingress nginx retirement](https://kubernetes.io/blog/2025/11/11/ingress-nginx-retirement/) in March 2026, so RustFS adds support for [gateway api](https://gateway-api.sigs.k8s.io/). Currently, RustFS only supports traefik as gateway class, more and more gateway class support will be added in the future after those classes are tested. If you want to enable gateway api, specify `gatewayApi.enabled` to `true` while specify `ingress.enabled` to `false`. After installation, you can find the `Gateway` and `HttpRoute` resources,

```
$ kubectl -n rustfs get gateway
NAME             CLASS     ADDRESS   PROGRAMMED   AGE
rustfs-gateway   traefik             True         169m

$ kubectl -n rustfs get httproute
NAME           HOSTNAMES            AGE
rustfs-route   ["example.rustfs.com"]   172m
```

Then, via RustFS instance via `https://example.rustfs.com` or `http://example.rustfs.com`.

# Uninstall

Uninstalling the rustfs installation with command,

```
helm uninstall rustfs -n rustfs
```
