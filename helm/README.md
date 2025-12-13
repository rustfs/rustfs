# RustFS Helm Mode

RustFS helm chart supports **standalone and distributed mode**. For standalone mode, there is only one pod and one pvc; for distributed mode, there are two styles, 4 pods and 16 pvcs(each pod has 4 pvcs), 16 pods and 16 pvcs(each pod has 1 pvc). You should decide which mode and style suits for your situation. You can specify the parameters `mode` and `replicaCount` to install different mode and style.

- **For standalone mode**: Only one pod and one pvc acts as single node single disk; Specify parameters `mode.standalone.enabled="true",mode.distributed.enabled="false"` to install.
- **For distributed mode**(**default**): Multiple pods and multiple pvcs, acts as multiple nodes multiple disks, there are two styles:
    - 4 pods and each pods has 4 pvcs(**default**)
    - 16 pods and each pods has 1 pvc: Specify parameters `replicaCount` with `--set replicaCount="16"` to install.

**NOTE**: Please make sure which mode suits for you situation and specify the right parameter to install rustfs on kubernetes.

# Parameters Overview

| parameter | description | default value |
| -- | -- | -- |
| replicaCount                      | Number of cluster nodes.                                           |  `4`.           |
| imagePullSecrets                  | A List of secrets to pull image from private registry.             | `name: secret-name`|
| imageRegistryCredentials.enabled  | To indicate whether pull image from private registry.              | `false`         |
| imageRegistryCredentials.registry | Private registry url to pull rustfs image.                         | None            |
| imageRegistryCredentials.username | The username to pull rustfs image from private registry.           | None            |
| imageRegistryCredentials.password | The password to pull rustfs image from private registry.           | None            |
| imageRegistryCredentials.email    | The email to pull rustfs image from private registry.              | None            |
| mode.standalone.enabled           | RustFS standalone mode support, namely one pod one pvc.            | `false`         |
| mode.distributed.enabled          | RustFS distributed mode support, namely multiple pod multiple pvc. | `true`          |
| image.repository                  | RustFS docker image repository.                                    | `rustfs/rustfs` |
| image.tag                         | The tag for rustfs docker image                                    | `latest`        |
| secret.rustfs.access_key          | RustFS Access Key ID                                               | `rustfsadmin`   |
| secret.rustfs.secret_key          | RustFS Secret Key ID                                               | `rustfsadmin`   |
| storageclass.name                 | The name for StorageClass.                                         | `local-path`    |
| storageclass.dataStorageSize      | The storage size for data PVC.                                     | `256Mi`         |
| storageclass.logStorageSize       | The storage size for log PVC.                                      | `256Mi`         |
| ingress.className                 | Specify the ingress class, traefik or nginx.                       | `nginx`         |


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

- The chart default pull rustfs image from dockerhub, if your rustfs image stores in private registry, you can use either existing image Pull secrets with parameter `imagePullSecrets` or create one setting `imageRegistryCredentials.enabled` to `true`,and then specify the `imageRegistryCredentials.registry/username/password/email` as well as `image.repository`,`image.tag` to pull rustfs image from your private registry.

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
rustfs   nginx   your.rustfs.com   10.43.237.152   80, 443   29m
```

Access the rustfs cluster via `https://your.rustfs.com` with the default username and password `rustfsadmin`.

> Replace the `your.rustfs.com` with your own domain as well as the certificates.

# TLS configuration

By default, tls is not enabled.If you want to enable tls(recommendated),you can follow below steps:

* Step 1: Certification generation

You can request cert and key from CA or use the self-signed cert(**not recommendated on prod**),and put those two files(eg, `tls.crt` and `tls.key`) under some directory on server, for example `tls` directory.

* Step 2: Certification specifying

You should use `--set-file` parameter when running `helm install` command, for example, running the below command can enable ingress tls and generate tls secret:

```
helm install rustfs rustfs/rustfs -n rustfs --set tls.enabled=true,--set-file tls.crt=./tls.crt,--set-file tls.key=./tls.key
```

# Uninstall

Uninstalling the rustfs installation with command,

```
helm uninstall rustfs -n rustfs
```
