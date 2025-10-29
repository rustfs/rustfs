# rustfs-helm

You can use this helm chart to deploy rustfs on k8s cluster.

## Parameters Overview

| parameter | description | default value |
| -- | -- | -- |
| replicaCount   | Number of cluster nodes.   |  Default is `4`. |
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
* RustFS >= 1.0.0-alpha.66

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
rustfs   nginx   xmg.rustfs.com   10.43.237.152   80, 443   29m
```

Access the rustfs cluster via `https://xmg.rustfs.com` with the default username and password `rustfsadmin`.

> Replace the `xmg.rustfs.com` with your own domain as well as the certificates.

## Uninstall

Uninstalling the rustfs installation with command,

```
helm uninstall rustfs -n rustfs
```
