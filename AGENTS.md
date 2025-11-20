## Repositorio nacional "CIENCIA_MX"

Estoy realizando un proyecto donde quiero crear un buscador de repositorios nacionales, el objetivo es tener en una pagina un buscador de repositorios oai-pmh de investgacion, tesis, documentos entre otros documentos de investigación. 

Cualquier persona puede entrar, buscar la informacion de su interes, entrar al item y tener una preview de dicha investigacion. 

## RECURSOS QUE UTILIZAMOS

- Contamos con una cantidad de endpoints OAI-IMP de diferentes instituciones de Mexico.
- Actualmente ya contamos con metadata en formato JSON. 

## CAMPOS QUE MOSTRAREMOS EN "CIENCIA_MX"

En este momento, nuestra metadata cuenta con diferentes etiquetas extraidas con el verb=ListRecords. 
En CIENCIA_MX nos interesa mostrar las siguientes etiquetas Dublin Core: 

-- dc:title
-- dc:creator
-- dc:subject
-- dc:description
-- dc:date
-- dc:type
-- dc:format
-- dc:identifier
-- dc:language
-- dc:rights

Dentro de la metadata existen mas etiquetas, pero nos regimos bajo estas 10. 

## EJEMPLOS DE ALGUNOS CAMPOS DUBLIN CORE: 

### dc:title
1. "dc:title": "Metodología Basada en Modelos de Red de Poros para la Estimación de Propiedades Efectivas de Flujo en Medios Porosos: Caso de Estudio para una Arenisca",

2. "dc:title": "Towards a taxonomy for public communication of science activities"

3. "dc:title": "INTERACCIÓN DE POTASIO Y LA SALINIDAD CAUSADA POR CLORURO DE SODIO EN PIMIENTO (Capsicum annuum L.) EN CULTIVO SIN SUELO./",

### dc:creator
1. "dc:creator": [
{
    "@id": "info:eu-repo/dai/mx/orcid/0000-0001-7894-7665",
    "#text": "Maxfield, S."
},
{
    "@id": "info:eu-repo/dai/mx/curp/MAXM780914MNEGXR02",
    "#text": "Sousa, M. M."
}
],
2. "dc:creator": {
    "@id": "info:eu-repo/dai/mx/cvu/217235",
    "#text": "MARIA DEL CARMEN SANCHEZ MORA"
},
3. "dc:creator": [
    "Osorio Velazquez, Jorge Miguel",
    "Eustaquio, Pech Ku",
    "Osorio Velazquez, Jorge Miguel",
    "Eustaquio, Pech Ku"
],

### dc:subject
1. "dc:subject": [
    "Instituto Mexicano del Seguro Social",
    "Seguridad social",
    "CIENCIAS SOCIALES::CIENCIAS JURÍDICAS Y DERECHO::OTRAS ESPECIALIDADES JURÍDICAS"
],
2. "dc:subject": "PERROS",
3. "dc:subject": [
    "info:eu-repo/classification/LCSH/Global Financial Crisis, 2008-2009.",
    "info:eu-repo/classification/LCSH/Banks and banking -- Government policy.",
    "info:eu-repo/classification/cti/5",
    "info:eu-repo/classification/cti/5"
],

### dc:description
1. "dc:description": [
    "Generalmente la crisis global financiera ha tendido relativamente un impacto limitado en los sistemas financieros en Latinoamérica. Este documento examina las explicaciones del comparativamente modesto impacto de la crisis global financiera en el sistema financiero mexicano. Una de ella es que el sistema financiero mexicano no sufrió de contagio porque este no era muy sofisticado ni tampoco lo global y suficientemente integrado. La otra hipótesis es que la historia de la crisis financiera fomentó regulaciones efectivas que mitigaron la carga global hacia el sector bancario basado en el mercado en el caso de México y explica porqué el sistema financiero fue relativamente indemne.",
    "Generally the global financial crisis had relatively limited impact on Latin American financial systems. The effect of the crisis on the real economy in Latin America traveled through trade rather than finance. This paper examines explanations for the comparatively modest impact of the global crisis on the Mexican financial system. It explores two different hypotheses. One is that the Mexican financial system did not suffer contagion because it was not very sophisticated or globally integrated. The other hypothesis is that the history of financial crisis encouraged effective regulations that mitigated the global charge toward market-based banking in the Mexico case and explains why the financial system was relatively unscathed by the crisis."
],
2. "dc:description": "Publicación eletrónica. Volumen 15 número 02, 2016",
3. "dc:description": "\"Es una enfermedad altamente prevaleciente en el ganado lechero, y es una de las enfermedades más importantes que afecta mundialmente la industria lechera; ocasionando pérdidas económicas muy fuertes a todos los productores de leche en el mundo (Rabello et al., 2005) debido a la disminución en el rendimiento de leche y un aumento en el número de tratamientos clínicos y desecho temprano de vacas (Ceron- Muñoz et al., 2002). Por lo que se ha reconocido, durante algún tiempo, como la enfermedad más costosa en los hatos lecheros (Correa et al., 2002; Boulanger et al., 2003). Los trabajos de investigación realizados en torno a la mastitis han enfocado en la identificación de los microorganismos patógenos causantes de la misma, pruebas de sensitividad de estos patógenos a diferentes antibióticos, evaluación de prácticas de manejo y la tasa de incidencia de mastitis en regiones específicas, comparación de diversos desinfectantes para el control de la mastitis así como la determinación de la relación cuantitativa entre los RCS y la producción de leche Boulanger et al., 2003). El Objetivos. De este documento, es describir las características de las infecciones intramamarias contagiosas, los esfuerzos de manejo y procedimientos específicos de control para reducir la tasa de nuevas infecciones por estos organismos y Discutir la evaluación de resultados de los tratamientos usados para mastitis Clínica. Los cuales fueron satisfactorios Como resultado y parte de un trabajo experimental de experiencia profesional\"",


### dc:date
1. "dc:date": "2016-01-08",
2. "dc:date": [
    "2020-03-06T18:32:13Z",
    "2020-03-06T18:32:13Z",
    "2018"
    ],
3. "dc:date": "2012",

### dc:type
1. "dc:type": "info:eu-repo/semantics/workingPaper",
2. "dc:type": "info:eu-repo/semantics/article",
3. "dc:type": [
    "Trabajo terminal, especialidad",
    "academicSpecialization"
],

### dc:format
1. "dc:format": "pdf",
2. "dc:format": "application/pdf"
3. "dc:format": [
    "pdf",
    "application/pdf"
],

### dc:identifier
1. "dc:identifier": "https://ru.ameyalli.dgdc.unam.mx/handle/123456789/75",
2. "dc:identifier": [
    "979-8826230343",
    "http://hdl.handle.net/20.500.12249/2875"
],
3. "dc:identifier": "http://repositorio.uaaan.mx:8080/xmlui/handle/123456789/47992",

### dc:language
1. "dc:language": "Español",
2. "dc:language": "spa",
3. "dc:language": "esp..-"
4. "dc:language": "es-mx",

### dc:rights
1. "dc:rights": [
    "Acceso abierto",
    "http://creativecommons.org/licenses/by-nc-nd/2.5/mx/"
],
2. "dc:rights": [
    "https://creativecommons.org/licenses/by-nc-nd/4.0/deed.es",
    "info:eu-repo/semantics/OpenAccess"
],
3."dc:rights": [
    "Acceso Abierto",
    "CC BY-NC-ND - Atribución-NoComercial-SinDerivadas"
],

## CONFLICTOS DE LAS ETIQUETAS
El contenido de las etiquetas es muy diverso: 
-- El Contenido de las etiquetas Dublin Core es muy diverson y variado (complicado de unificar)
-- Una clave (etiqueta DC) puede tener un valor como String o un array de Strings
-- El valor de una clave (etiqueta DC) puede estar vacio
-- Una clave puede tener valores logicos (nombres, fechas, titulos ...) o links

## PROXIMOS RETOS
Tenemos que construir una base en postgresql (normalizacion sencilla) que nos permita acceder a toda la informacion con querys

### PRE-CARGA A LA BASE 
#### Unificar datos **SEGUNDA PRIORIDAD**
Necesitamos unificar datos, Pasar de X -> Y (Y es el campo que se carga a la base): 
--- **dc:creator**:  "ANGEL MANUEL" -> "Angel Manuel"
--- **dc:language**: "spa","eng","Español" -> "Español", "Ingles" (aun por definir)
--- **dc:subject**: "info:eu-repo/classification/cti/5" -> Un subject logico (investigar que significa dicho link)
--- **dc:format**: "application/pdf", "PDF" -> "pdf" (mismo caso para otro tipo de formato)
--- **dc:rights**: ¿Como unificamos las referencias? 
#### Como cargamos (**TERCER PRIORIDAD**)
Un script listo en python que me permita la carga a una base de postgresql
-- Tiene que ser flexible, por la variedad de los datos
-- Debe estan en un paradigma POO

### CARGA A LA BASE
#### TABLA NORMALIZADA (SENCILLA PARA PRIMER SAQUE) **ALTA PRIORIDAD**
Necesitamos una base de datos normalizada que pueda organizar los datos.
Posibles tablas (**para debate con IA**): 
-- CREATORS: tabla de creadores con item_creator, creator_name, ...
-- ITEMS: tabla para items donde herede item_creator, item_repository, tenga su propio identificador (puede ser el dc:identifier solo si en el JSON, la etiqueta viene como String y no como Array de Strings)
-- REPOSITORYS: tabla de repositorios donde contenga item_repository, name_repository (nombre del repositorio), software (DSPACE, GEONETWORK,EPRINTS), version (version del software), alive (boolean para determinar si el endpoint sigue disponible), endpoint

### POST-CARGA **CUARTA/BAJA PRIORIDAD**
#### SENTENCIAS BASICAS 
Definir una serie de sentencias sql para visualizar datos
#### QUERYS
Definir querys para obtener data importante. Ejemplos: 
-- Autores con mas contribuciones (items)
-- Las contribuciones mas nuevas / antiguas
