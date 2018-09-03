# Skbn

Skbn is a tool for copying files and directories between Kubernetes and AWS S3. It is named after the 1981 video game [Sokoban](https://en.wikipedia.org/wiki/Sokoban).

## Install

```
wget -qO- https://github.com/maorfr/skbn/releases/download/0.1.0/skbn.tar.gz | sudo tar xvz -C /usr/local/bin
```

## Build from source

Skbn uses [glide](https://github.com/Masterminds/glide) as a dependency management tool, since some of the referenced packages are not available using [dep](https://github.com/golang/dep).

```
glide up
go build -o skbn cmd/skbn.go
```


## Usage

### Copy files from Kubernetes to S3

```
skbn cp \
    --src k8s://<namespace>/<podName>/<containerName>/<path> \
    --dst s3://<bucket>/<path>
```

### Copy files from S3 to Kubernetes

```
skbn cp \
    --src s3://<bucket>/<path> \
    --dst k8s://<namespace>/<podName>/<containerName>/<path>
```

## Credentials


### Kubernetes

Skbn tries to get credentials in the following order:
1. if `KUBECONFIG` environment variable is set - skbn will use the current context from that config file
2. if `~/.kube/config` exists - skbn will use the current context from that config file with an [out-of-cluster client configuration](https://github.com/kubernetes/client-go/tree/master/examples/out-of-cluster-client-configuration)
3. if `~/.kube/config` does not exist - skbn will assume it is working from inside a pod and will use an [in-cluster client configuration](https://github.com/kubernetes/client-go/tree/master/examples/in-cluster-client-configuration)


### AWS

Skbn uses the default AWS [credentials chain](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html).

## Examples

1. [In-cluster example](https://github.com/maorfr/skbn/tree/master/examples/in-cluster)
2. [Code example](https://github.com/maorfr/skbn/tree/master/examples/code)