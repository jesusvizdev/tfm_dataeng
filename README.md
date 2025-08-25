
## 📄 Descripción del proyecto

Este repositorio contiene todo el código y artefactos desarrollados para el proyecto del TFM, enfocado en la implementación de un pipeline de datos completo para el dataset NASA CMAPSS.

Incluye:

- **Pipeline batch (Medallion Architecture)**: Código modular en Python que implementa las fases **Landing → Bronze → Silver → Gold**, preparado para empaquetarse en un archivo `.tar.gz` e importarse como librería en Azure Databricks. Junto al código, se incluyen los notebooks utilizados para ejecutar y validar cada etapa del pipeline dentro de Databricks.

- **Pipeline streaming**: Implementación dockerizada que simula la generación de datos de sensores en tiempo real, enviándolos a Azure Event Hubs para su posterior procesamiento y aplicación del modelo de predicción de RUL (Remaining Useful Life) entrenado en la fase batch.

En conjunto, este repositorio reúne tanto el desarrollo batch como la parte de streaming, cubriendo el ciclo completo desde la ingesta de datos hasta la generación de alertas basadas en predicciones.

## 📂 Estructura principal del repositorio

```
cmaps_pipeline/ # Librería Python con el código modular del pipeline batch
│
├── medallion/ # Implementación de las fases Bronze, Silver y Gold
│
├── model/ # Código relacionado con el modelo de predicción de RUL. Adjunta el modelo generado en Azure
│
data/ # Directorio local con el dataset original en 'landing' y los directorios medallion
│
notebooks/ # Notebooks de Databricks usados en Azure
│
iot_generator/ # Código del productor de datos en streaming
├── azure_function/ # Contiene las Azure Functions usadas para enviar alertas en tiempo en real a un dominio Gmail.
│
tests/ # Tests unitarios para los módulos del pipeline
│
dist/ # Archivos de distribución generados por setup.py y usados por los notebooks de Databricks
│
Dockerfile # Dockerfile para el entorno de streaming
setup.py # Script para empaquetar e instalar la librería Python
```

---

## ⚙️ Notas de ejecución

Este repositorio contiene el código real utilizado en Azure para el desarrollo del TFM, pero no es un paquete ejecutable de forma directa sobre un entorno local sin configuración previa en la nube.  

Para propósitos de prueba, se pueden realizar las siguientes acciones:

- **Pipeline batch**:  
Es posible simular la ejecución completa del pipeline batch de forma local mediante el script run_batch.py.
Este script ejecuta de forma secuencial las fases **Landing → Bronze → Silver → Gold** usando los datos en `data/landing` como entrada y generando la salida en `data/gold`.  
En Azure, el entrenamiento del modelo se ha realizado usando el clúster de Databricks, por lo que en este repositorio únicamente se incluye el código de entrenamiento y el modelo ya entrenado exportado desde Azure.

- **Pipeline streaming**:  
El código streaming no se puede ejecutar directamente en local sin la configuración de **Azure Event Hubs**.  
Para pruebas, se proporciona un `Dockerfile` que empaqueta el productor de datos (`iot_generator/main.py`) y permite enviar datos simulados a Event Hubs, reproduciendo el flujo de datos en tiempo real usado en Azure.

En resumen, este repositorio es una referencia completa del desarrollo, con capacidad de prueba parcial en local (pipeline batch simulado y productor streaming dockerizado).


## 🚀 Guía de ejecución



### Ejecutar el pipeline batch en local

* Cree y active un entorno virtual:

```bash
python -m venv venv
venv\Scripts\activate   
```

* Instale las dependencias:

```bash
pip install -r requirements.txt
```

* Ejecute el pipeline batch completo:

```bash
python run_batch.py
```

Este script leerá los datos de data/landing, los procesará a través de las fases Landing → Bronze → Silver → Gold y dejará la salida en data/gold.

### Construir y ejecutar el productor streaming (Docker)

```bash
docker build -t iot-sensor-producer .
```
Ejecutar el contenedor (conexión a Event Hubs):

```bash
docker run --rm iot-sensor-producer
```
**Nota:** El contenedor Docker se puede ejecutar en local y mostrará por consola los datos simulados que se estarían enviando a Event Hubs. No se puede comprobar el flujo completo hacia Azure porque el Event Hub es privado. Además, el Endpoint de conexión a EventHubs se encuentra en un .env, dado que GitHub bloquea el código con Azure Secrets"
