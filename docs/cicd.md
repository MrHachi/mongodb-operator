# CI/CD and versioning scheme

## CI/CD workflows

### On PR to main

- Run lint check
- Execute unit tests
- Execute e2e tests
- Check CRD version
    - Adding a new API version requires a controller update
    - Breaking changes require a new API version
- Check controller version
    - must have been raised if code has been touched
    - new version MUST result in chart version
- Check chart version
    - must have been raised if templates have been touched
    - `appVersion` MUST match controller version

Not triggered on `.md` only PRs.

### On PR merge to main

- Push database and controller images to GHCR
- Push chart to GHCR and push the tag `chart-*`

### On tag push

- Tag: `mongodb-*-*`
    - push DB image to GHCR
- Tag: `controller-*`
    - push controller image to GHCR

Not triggered on `.md` only pushes.

### On release

- Tag: `mongodb-*-*`
    - retag image built from the tagged commit
- Tag: `controller-*`
    - retag image built from the tagged commit

## Versioning

Chart version follows SemVer.

appVersion matches the controller version.

MongoDB image version follows
<upstream-version>-r<revision>.

### Helm chart versioning

Pattern: `chart-MAJ.MIN.PAT`

### Controller versioning

Pattern: `controller-MAJ.MIN.PAT`

### Default MongoDB image versioning

Pattern: `mongodb-BASE_VERSION-rCUSTOM_REVISION`
