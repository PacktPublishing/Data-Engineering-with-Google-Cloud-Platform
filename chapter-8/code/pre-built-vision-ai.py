from google.cloud import vision
from google.cloud import translate_v2 as translate

# TODO: Change to your gcs bucket
GCS_BUCKET = "packt-data-eng-on-gcp-data-bucket"
gcs_uri = "gs://{}/chapter-8/chapter-8-example-text.jpg".format(GCS_BUCKET)

def detect_text(gcs_uri : str):
    print("Looking for text from image in GCS: {}".format(gcs_uri))

    image = vision.Image(
        source=vision.ImageSource(gcs_image_uri=gcs_uri)
    )

    text_detection_response = vision_client.text_detection(image=image)
    annotations = text_detection_response.text_annotations
    if len(annotations) > 0:
        text = annotations[0].description
    else:
        text = ""
    print("Extracted text : \n{}".format(text))

    detect_language_response = translate_client.detect_language(text)
    src_lang = detect_language_response["language"]
    print("Detected language {}".format(src_lang))

vision_client = vision.ImageAnnotatorClient()
translate_client = translate.Client()
detect_text(gcs_uri)
