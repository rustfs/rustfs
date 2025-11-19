# rustfs-helm

You can use this helm chart to deploy rustfs on k8s cluster. The chart supports standalone and distributed mode. For standalone mode, there is only one pod and one pvc; for distributed mode, there are two styles, 4 pods and 16 pvcs(each pod has 4 pvcs), 16 pods and 16 pvcs(each pod has 1 pvc). You should decide which mode and style suits for your situation. You can specify the parameters `mode` and `replicaCount` to install different mode and style.

## Parameters Overview

| parameter | description | default value |
| -- | -- | -- |
| replicaCount   | Number of cluster nodes.   |  Default is `4`. |
| mode.standalone.enabled | RustFS standalone mode support, namely one pod one pvc. | Default is `false` |
| mode.distributed.enabled | RustFS distributed mode support, namely multiple pod multiple pvc. | Default is `true`. |
| image.repository  | docker image repository.   |  rustfs/rustfs.  |
| image.tag | the tag for rustfs docker image | "latest" |
| secret.rustfs.access_key | RustFS Access Key ID | `rustfsadmin` |
| secret.rustfs.secret_key | RustFS Secret Key ID | `rustfsadmin` |
| storageclass.name | The name for StorageClass. | `local-path` |
| ingress.className | Specify the ingress class, traefik or nginx. | `nginx` |


**NOTE**: [`local-path`](https://github.com/rancher/local-path-provisioner) is used by k3s. If you want to use `local-path`, running the command,

```
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.32/deploy/local-path-storage.yaml
```

## Requirement

* Helm V3
* RustFS >= 1.0.0-alpha.68

## Installation

If your ingress class is `traefik`, running the command:

```
helm install rustfs -n rustfs --create-namespace ./ --set ingress.className="traefik"
```

If your ingress class is `nginx`, running the command:

```
helm install rustfs -n rustfs --create-namespace ./ --set ingress.className="nginx"
```

> `traefik` or `nginx`, the different is the session sticky/affinity annotations.

**NOTE**: If you want to install standalone mode, specify the installation parameter `--set mode.standalone.enabled="true",mode.distributed.enabled="false"`; If you want to install distributed mode with 16 pods, specify the installation parameter `--set replicaCount="16"`.

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

## Uninstall

Uninstalling the rustfs installation with command,

```
helm uninstall rustfs -n rustfs
```

