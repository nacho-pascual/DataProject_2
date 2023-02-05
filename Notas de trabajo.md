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

-Proximos pasos y decisiones:
    -¿Kw lo dejamos como str o convertimos a int?(No hay problema para hacer las lógicas pero el código queda menos limpio)
    -Decidir estructura para alertas:
        -Vamos a utilizar para todo la misma tabla? En este caso podríamos añadir las columnas de "estado"(normal/alerta),"tipo de alerta"(consumo anómalo/consumo excesivo por periodo) (Si vamos a separar por agregaciones no será posible)
        -Aun utilizando tablas diferentes podemos replicar la tabla añadiendo estas columnas mencionadas y hacer 1-2 más en funcion de las 
        agregaciones de las alertas.
    -Decidir lógica alertas:(Posibles ideas):
        -Alertas consumo anómalo: agregación del consumo por hora y establecer cantidad superior a x para un horario desde las 12 de la noche hasta las 8 de la mañana.
        -Alertas consumo excesivo por periodo:  agregación del consumo por 15 mins y marcar unos maximos para alerta.Ej: nos llegaría alerta si en periodo valle pasamos de los 3000kw en la agregación de 15mins.

        

