- Día 1 de febrero:

1º) Modificaciones en 02_Code > generator.py:
    * Modificamos def generateMockData() para generar nuestros datos relativos al consumo de electricidad:

2º) Configuramos nuestro proyecto en GCP:
    * Creamos el topic y la subscripción
    * Lanzamos el generator.py con nuestro topic y project_id

3º) Dataflow:
    * Añadimos las referencias a nuestro proyecto (topic, subscription, project id) en DataflowCode.py 
    * Creamos el schema
    * Ejecutamos DataflowCode.py desde local
    * Solucionamos el problema de conectarnos a BigQuery configurando >gcloud config set project_psyched-freedom-376515

4º) Deberes para mañana:
    * Dataflow con google cloud a través de un template.
    * Empezar a testear Data Studio.
    * Limpiar código de dataflow (que coja variables para coger por parámetros y no tener que estar cambiando el código todo el rato).
    * Empezar a plantear la generación de datos con sentido.


