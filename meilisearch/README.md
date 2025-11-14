# MeiliSearch FastAPI Project

This project provides a FastAPI application with MeiliSearch integration for search queries.

## Setup

### Opción 1: Ejecutar localmente (sin Docker)
1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Start MeiliSearch (assuming it's installed locally):
   ```
   meilisearch
   ```
   It should run on http://localhost:7700 by default.

3. Run the FastAPI app:
   ```
   uvicorn main:app --reload
   ```

### Opción 2: Ejecutar con Docker Compose
1. Asegúrate de tener Docker y Docker Compose instalados.

2. Construye y ejecuta los servicios:
   ```
   docker-compose up --build
   ```

   Esto iniciará MeiliSearch en el puerto 7700 y la app FastAPI en el puerto 8000.

3. Para detener:
   ```
   docker-compose down
   ```

The app will load the mock data from `data/mock_data.json` into MeiliSearch on startup.

## Endpoints

- `GET /search/partial?q=<query>` - Partial search returning up to 3 results
- `GET /search/total?q=<query>` - Total search returning all matching results

## Data

### Datos de prueba
Mock data is stored in `data/mock_data.json`.

### Datos de producción
Los datos de producción se encuentran en archivos JSON dentro de `data/20251113_200813/`.

Para procesar y consolidar todos los datos:
```bash
python3 process_data.py
```

Esto generará `prod_data/consolidated_data.json` con todos los registros académicos consolidados.

Para cargar los datos manualmente en MeiliSearch (si ya está corriendo):
```bash
python3 load_data.py
```

**Nota**: Al usar Docker Compose, los datos se cargan automáticamente al iniciar la aplicación.