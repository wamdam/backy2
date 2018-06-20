#!/usr/bin/env bash

: ${BACKUP_SELECTOR:=nomatch==matchnot}
: ${BACKUP_RETENION:=latest3,hours24,days30,months3}
: ${PROM_PUSH_GATEWAY:=:9091}

cd "$(dirname "${BASH_SOURCE[0]}")" 

. /benji/bin/activate
. prometheus.sh
. metrics.sh
. ceph.sh

# Can't use "u" because prometheus.bash fails when using it
set -eo pipefail

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
	
	backup::ceph "$NAME" "$CEPH_POOL" "$CEPH_IMAGE"
	benji_job_start_time -action=enforce -type= -version_name="$NAME" set $(date +'%s.%N')
	benji enforce "$BACKUP_RETENION" "$NAME"
	benji_job_completion_time -action=enforce -type= -version_name="$NAME" set $(date +'%s.%N')
done

benji_job_start_time -action=cleanup -type= -version_name= set $(date +'%s.%N')
benji cleanup
benji_job_completion_time -action=cleanup -type= -version_name= set $(date +'%s.%N')

io::prometheus::ExportAsText
io::prometheus::Push job=benji gateway="$PROM_PUSH_GATEWAY"