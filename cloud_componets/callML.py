import googleapiclient.discovery
from flask import jsonify
def predict_json(request):
    """Send json data to a deployed model for prediction.
    Function dependencies:
    google-api-python-client
    """
    project = 'airlinegcp'
    model = 'wdl'
    version = 'v1'
    instances = request.get_json()
    service = googleapiclient.discovery.build('ml', 'v1')
    name = 'projects/{}/models/{}'.format(project, model)

    name += '/versions/{}'.format(version)

    response = service.projects().predict(
        name=name,
        body=instances
    ).execute()

    if 'error' in response:
        raise RuntimeError(response['error'])

    return jsonify(response)