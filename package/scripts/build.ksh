if [ -z "$DOCKER_HOST" ]; then
   DOCKER_TOOL=docker
else
   DOCKER_TOOL=docker-legacy
fi

# set the definitions
INSTANCE=easystore-tools
NAMESPACE=uvadave

# build the image
$DOCKER_TOOL build -f package/Dockerfile -t $NAMESPACE/$INSTANCE .

# return status
exit $?
