#!/usr/bin/env bash

: "${BACKUP_SELECTOR:=nomatch==matchnot}"
: "${BACKUP_RETENION:=latest3,hours24,days30,months3}"
: "${PROM_PUSH_GATEWAY:=:9091}"
: "${DEEP_SCRUBBING_ENABLED:=1}"
: "${DEEP_SCRUBBING_VERSIONS_PERCENTAGE:=6}"
: "${DEEP_SCRUBBING_BLOCKS_PERCENTAGE:=50}"
: "${SCRUBBING_ENABLED:=0}"
: "${SCRUBBING_VERSIONS_PERCENTAGE:=6}"
: "${SCRUBBING_BLOCKS_PERCENTAGE:=50}"

cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

. prometheus.sh
. metrics.sh
. tryCatch.sh
. common.sh
. ceph.sh

# Get a list of persistent volumes (this will also filter out volumes not provisioned by Rook)
PVS=$(kubectl get pvc --all-namespaces -l "$BACKUP_SELECTOR" -o json | jq -r '.items | map(select(.metadata.annotations."volume.beta.kubernetes.io/storage-provisioner"=="rook.io/block") | .spec.volumeName) | .[]')

# Now try to find the image name (which currently is identical to the PV name)
for pv in $PVS
do
	set -- $(kubectl get pv "$pv" -o jsonpath='{.spec.claimRef.namespace}{" "}{.spec.claimRef.name}{" "}{.spec.flexVolume.options.pool}{" "}{.spec.flexVolume.options.image}')
	PV_NAMESPACE="$1"
	PV_NAME="$2"
	CEPH_POOL="$3"
	CEPH_IMAGE="$4"
	NAME="$PV_NAMESPACE/$PV_NAME"
	
	benji::backup::ceph "$NAME" "$CEPH_POOL" "$CEPH_IMAGE"
	benji::enforce "$BACKUP_RETENION" "$NAME"
done

benji::cleanup

if [[ $DEEP_SCRUBBING_ENABLED == 1 ]]
then
    benji::bulk_deep_scrub "$DEEP_SCRUBBING_VERSIONS_PERCENTAGE" "$DEEP_SCRUBBING_BLOCKS_PERCENTAGE"
fi

if [[ $SCRUBBING_ENABLED == 1 ]]
then
    benji::bulk_scrub "$DEEP_SCRUBBING_VERSIONS_PERCENTAGE" "$DEEP_SCRUBBING_BLOCKS_PERCENTAGE"
fi

echo
io::prometheus::ExportAsText
io::prometheus::Push job=benji gateway="$PROM_PUSH_GATEWAY"
