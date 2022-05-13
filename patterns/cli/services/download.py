from requests.models import Response


def download_graph_version(name: str, organization_name: str) -> Response:
    # manifest = GraphManifestBuilder(
    #     directory=pth_to_root, cfg=cfg
    # ).build_manifest_from_config()
    # zipf = compress_directory(pth_to_root)
    # b64_zipf = base64.b64encode(zipf.read())
    # resp = post(
    #     Endpoints.GRAPH_VERSIONS_UPLOAD,
    #     data={"graph_manifest": manifest, "zip": b64_zipf.decode()},
    # )
    # return resp
    pass
