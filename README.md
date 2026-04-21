# Switchboard

Opinionated web panel for homelab Kubernetes. Binary start/stop per workload, group controls, node cordon, preferred-node pinning, and RAM-aware cluster rebalancing.

Not another k9s or Lens: Switchboard is built for small clusters where you want to pulse services on and off from a browser, not manage autoscaling. Think of it as the operator panel for your homelab.

## Features

- **Workload list** across all namespaces (Deployments, StatefulSets, DaemonSets) with per-workload RAM/CPU, node placement, and Traefik ingress URLs.
- **Binary scaling**: one-click start/stop (replicas 0 or 1). No HPA, no N-replica scaling.
- **Restart** any workload.
- **Groups**: namespaces show up grouped in the UI; start/stop a whole group at once.
- **Node cordon/uncordon** from the UI.
- **Preferred node per workload**: pins soft `nodeAffinity` + a persistent annotation (survives re-applies).
- **Spread evenly**: best-fit-decreasing by RAM across schedulable nodes.
- **Health endpoint** per workload at `/api/ping/<ns>/<name>` (returns 200 when running, 503 otherwise). Useful for dashboards.

## Deploy

```bash
kubectl apply -f https://raw.githubusercontent.com/josecriane/switchboard/main/deploy/manifests.yaml
```

The manifest creates a `switchboard` namespace, a ClusterRole with the needed permissions, a Deployment, and a ClusterIP Service. Expose it behind your own Ingress or IngressRoute.

Image: `ghcr.io/josecriane/switchboard:latest` (multi-arch: amd64, arm64).

### Configuration

Switchboard reads `/config/services.json` on startup. The default ConfigMap ships an empty config:

```json
{
  "groupNames": { "media": "Media" },
  "noStop": ["kube-system/*", "default/critical-thing"],
  "hide": ["kube-system", "kube-public"]
}
```

- `groupNames`: map of namespace -> display name in the UI. Unset namespaces are Title-Cased.
- `noStop`: entries (`ns/name` or `ns/*`) that cannot be scaled to 0. Useful for critical infra.
- `hide`: namespaces to hide entirely.

Update the ConfigMap and restart the pod to apply.

### Ingress example (Traefik IngressRoute)

```yaml
apiVersion: traefik.io/v1alpha1
kind: IngressRoute
metadata:
  name: switchboard
  namespace: switchboard
spec:
  entryPoints: [websecure]
  routes:
    - match: Host(`switchboard.example.com`)
      kind: Rule
      services:
        - name: switchboard
          port: 8080
  tls:
    secretName: wildcard-tls
```

Switchboard has no built-in auth. Put it behind your SSO proxy (Authentik ForwardAuth, oauth2-proxy, Tailscale Serve, etc.).

## Using with Nix

Switchboard ships a flake. Add it as an input:

```nix
{
  inputs.switchboard.url = "github:josecriane/switchboard";

  outputs = { switchboard, ... }: {
    # switchboard.packages.${system}.default       -> Go binary
    # switchboard.packages.${system}.dockerImage   -> OCI tarball (for k3s ctr import)
    # switchboard.packages.${system}.manifests     -> derivation with manifests.yaml
  };
}
```

## Annotations

Switchboard writes two annotations on workloads it manages:

- `switchboard.io/user-paused: "true"` — set when the user toggles a workload off.
- `switchboard.io/preferred-node: <node-name>` — current preferred node (paired with soft nodeAffinity).

These survive rebuilds; external tooling can read them to preserve user intent.

## Development

```bash
nix develop
go run .
```

Or without Nix: Go 1.22+, run `go build && ./switchboard`. Set `KUBERNETES_SERVICE_HOST` and `KUBERNETES_SERVICE_PORT` and mount a service-account token at `/var/run/secrets/kubernetes.io/serviceaccount/token` to talk to a real cluster.

## License

Apache 2.0. See [LICENSE](LICENSE).
