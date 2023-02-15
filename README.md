# VIDAMAJUNA

![LOGO](./Imagenes/logo_vidamajuna.png)

# Data Project 2
## Máster en Data Analytics - EDEM
### Curso 2022/2023

- [Victor Ruiz](https://www.linkedin.com/in/vruizext/)
- [Darío Fernández](https://www.linkedin.com/in/dar%C3%ADo-fern%C3%A1ndez-fern%C3%A1ndez/)
- [Marina Pérez](https://www.linkedin.com/in/marinaperezbarber/)
- [Julio Sahuqillo](https://www.linkedin.com/in/juliosahuquillohuerta/)
- [Nacho Pascual](https://www.linkedin.com/in/nacho-pascual/)

# Proyecto
## Contexto
EDEM ha creado el día 18 de Marzo un evento de lanzamiento de empresas con productos IoT. Es vuestro momento! En este evento podréis presentar vuestro producto IoT como SaaS. Durante estas tres semanas, debéis pensar un producto IoT, desarrollarlo y simular su uso. Este proyecto debe tener una arquitectura escalable pero no es obligatorio cloud, vosotros decidís. De cara a participar en este evento, vuestra solución debe ser escalable, opensource y cloud.

## ¿Qué es VIDAMAJUNA?

VIDAMAJUNA es una startup enfocada al ahorro energético para las oficinas.
 

# Tecnología
## Arquitectura

![ARQUITECTURA](./Imagenes/Diagrama de arquitectura.png)


## Configuración del sistema

**PubSub**
En primer lugar, creamos un Topic y la suscripción por defecto.

Desde la página PubSub de Cloud Console, creamos el Topic con un nombre único y añadimos una suscripción por defecto. Tanto los Topics como las suscripciones son necesarios en los siguientes pasos para crear la canalización de datos.



**DataFlow**


**Cloud Functions**


**BigQuery**


**Data Studio**

Finalmente, desde Data Studio se leen directamente los datos de BigQuery y se muestran en un dashboard interactivo.


**Video del funcionamiento**
