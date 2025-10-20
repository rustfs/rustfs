# Install rustfs with mnmd mode using ansible

This chapter show how to install rustfs with mnmd(multiple nodes multiple disks) using ansible playbook.Two installation method are available, namely binary and docker compose.

## Requirements

- Multiple nodes(At least 4 nodes,each has private IP and public IP)
- Multiple disks(At least 1 disk per nodes, 4 disks is a better choice)
- Ansible should be available
- Docker should be available(only for docker compose installation)

## Binary installation and uninstallation

### Installation

For binary installation([script installation](https://rustfs.com/en/download/),you should modify the below part of the playbook,

```
- name: Modify Target Server's hosts file
  blockinfile:
    path: /etc/hosts
    block: |
      172.92.20.199  rustfs-node1
      172.92.20.200  rustfs-node2
      172.92.20.201  rustfs-node3
      172.92.20.202  rustfs-node4
```

Replacing the IP with your nodes' **private IP**.If you have more than 4 nodes, adding the ip in order.

Running the command to install rustfs

```
ansible-playbook --skip-tags rustfs_uninstall binary-mnmd.yml
```

After installation success, you can access the rustfs cluster via any node's public ip and 9000 port. Both default username and password are `rustfsadmin`.


### Uninstallation

Running the command to uninstall rustfs

```
ansible-playbook --tags rustfs_uninstall binary-mnmd.yml
```

## Docker compose installation and uninstallation

**NOTE**: For docker compose installation,playbook contains docker installation task,

```
tasks:
  - name: Install docker
    shell: |
          apt-get remove -y docker docker.io containerd runc || true
          apt-get update -y
          apt-get install -y ca-certificates curl gnupg lsb-release
          install -m 0755 -d /etc/apt/keyrings
          curl -fsSL https://mirrors.aliyun.com/docker-ce/linux/ubuntu/gpg | gpg --dearmor --yes -o /etc/apt/keyrings/docker.gpg
          chmod a+r /etc/apt/keyrings/docker.gpg
          echo \
            "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://mirrors.aliyun.com/docker-ce/linux/ubuntu \
            $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
          apt-get update -y
          apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
    become: yes
    register: docker_installation_result
    changed_when: false

  - name: Installation check
    debug:
      var: docker_installation_result.stdout
```

If your node already has docker environment,you can add `tags` in the playbook and skip this task in the follow installation.By the way, the docker installation only for `Ubuntu` OS,if you have the different OS,you should modify this task as well.

For docker compose installation,you should also modify the below part of the playbook,

```
extra_hosts:
  - "rustfs-node1:172.20.92.202"
  - "rustfs-node2:172.20.92.201"
  - "rustfs-node3:172.20.92.200"
  - "rustfs-node4:172.20.92.199"
```

Replacing the IP with your nodes' **private IP**.If you have more than 4 nodes, adding the ip in order.

Running the command to install rustfs,

```
ansible-playbook --skip-tags docker_uninstall docker-compose-mnmd.yml
```

After installation success, you can access the rustfs cluster via any node's public ip and 9000 port. Both default username and password are `rustfsadmin`.

### Uninstallation

Running the command to uninstall rustfs

```
ansible-playbook --tags docker_uninstall docker-compose-mnmd.yml
```


