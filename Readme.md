# Proyecto ETL

Proyecto colaborativo para limpiar datos con Apache Hop y cargarlos en MySQL. 

## ¿Qué hay aquí?
- **Datos/** y **DatosSalida/**: Carpetas para los CSV originales y los procesados.
- **pipeline_examenes.hpl**: El flujo de Apache Hop.
- **docker-compose.yml**: Archivo para levantar la base de datos local rápidamente.

## Pasos para ejecutarlo
1. Abre una terminal en esta carpeta y levanta la base de datos con: 
   `docker compose up -d`
2. Abre Apache Hop y carga el archivo `.hpl`.
3. **Ojo:** Abre el primer nodo ("CSV file input") y cambia la ruta del archivo para que apunte a la carpeta Datos en tu compu, si no te va a dar error de lectura.
4. Corre el pipeline.

## Siguientes pasos (Pendientes)
- [ ] Agregar nodo para pasar textos a mayúsculas.
- [ ] Agregar nodo para limpiar acentos.
- [ ] Hacer el segundo pipeline del Parquet (salarios tech) que es el parquet que yo descargué, te lo dejo en la carpeta de datos, pero puedes usar el que tu hayas encontrado si quieres.