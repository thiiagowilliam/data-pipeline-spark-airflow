terraform {
  required_providers {
    k3d = {
      source = "SneakyBugs/k3d"
      version = "1.0.1"
    }
  }
}

provider "k3d" {
}

resource "k3d_cluster" "cluster" {
  name       = "example-cluster"
  k3d_config = <<EOF
apiVersion: k3d.io/v1alpha5
kind: Simple
metadata:
  name: datalake

servers: 1
agents: 3

kubeAPI:
  hostPort: "6443"

ports:
  - port: 80:80
    nodeFilters:
      - loadbalancer

registries:
  create:
    name: local-registry
    host: "0.0.0.0"
    hostPort: "5000"
  config: |
    mirrors:
      "docker.io":
        endpoint:
          - http://local-registry:5000

volumes:
  - volume: /tmp/k3d-storage:/var/lib/rancher/k3s/storage
    nodeFilters:
      - all

options:
  k3s:
    extraArgs:
      - arg: --node-taint=node-role.kubernetes.io/control-plane=
        nodeFilters:
          - server:*
      - arg: --kubelet-arg=eviction-hard=memory.available<100Mi,nodefs.available<1Gi,imagefs.available<1Gi
        nodeFilters:
          - all
      - arg: --disable=traefik,servicelb,metrics-server
        nodeFilters:
          - server:*
  kubeconfig:
    updateDefaultKubeconfig: true  # Atualiza kubeconfig automaticamente
    switchCurrentContext: true     # Muda para o contexto do cluster
  runtime:
    agentArgs:
      - --cpus=2  # Aloca até 2 CPUs por agent (ajuste se host tiver mais cores)
      - --memory=4g  # Aloca até 4GB por agent
    serverArgs:
      - --cpus=2
      - --memory=4g
EOF
}