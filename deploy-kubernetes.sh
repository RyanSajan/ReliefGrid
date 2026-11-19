#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf '%s\n' 'Usage: bash deploy-kubernetes.sh [--create-admin] [--no-port-forward]' \
        'Set MINIKUBE_PROFILE to select a profile or RELIEFGRID_PORT to change local port 8001.'
}

create_admin=false
port_forward=true
while (($#)); do
    case "$1" in
        --create-admin) create_admin=true ;;
        --no-port-forward) port_forward=false ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; exit 2 ;;
    esac
    shift
done

for command in docker minikube kubectl openssl sed; do
    if ! command -v "$command" >/dev/null; then
        printf 'Required command missing: %s\n' "$command" >&2
        exit 1
    fi
done

profile=${MINIKUBE_PROFILE:-minikube}
port=${RELIEFGRID_PORT:-8001}
if [[ ! $port =~ ^[1-9][0-9]{0,4}$ ]] || ((port > 65535)); then
    printf 'RELIEFGRID_PORT must be between 1 and 65535.\n' >&2
    exit 2
fi
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

k() {
    kubectl --context "$profile" "$@"
}

docker info >/dev/null
minikube start -p "$profile" --driver=docker

docker build -t emergency-app:local-build .
image_id=$(docker image inspect emergency-app:local-build --format '{{.Id}}')
image="emergency-app:${image_id#sha256:}"
docker tag emergency-app:local-build "$image"
minikube -p "$profile" image load "$image"

k create namespace emergency --dry-run=client -o yaml | k apply -f -
secret=$(k -n emergency get secret reliefgrid-auth --ignore-not-found -o name)
if [[ -z $secret ]]; then
    openssl rand -hex 32 | tr -d '\n' | k -n emergency create secret generic reliefgrid-auth \
        --from-file=AUTH_SECRET=/dev/stdin
fi

# A tag tied to the build updates existing pods when the image changes.
sed "s|image: emergency-app:latest|image: $image|" k8s/all.yaml | k apply -f -
deployments=$(k -n emergency get deployments -o name)
while IFS= read -r deployment; do
    k -n emergency rollout status "$deployment" --timeout=240s
done <<< "$deployments"
k -n emergency get deployments,pods,services,pvc

if [[ $create_admin == true ]]; then
    k -n emergency exec -it deployment/request-service -- python -m app.create_admin
fi
if [[ $port_forward == true ]]; then
    printf 'Dashboard: http://localhost:%s (Ctrl+C stops forwarding; Kubernetes stays running.)\n' "$port"
    k -n emergency port-forward service/request-service "$port:8000"
fi
