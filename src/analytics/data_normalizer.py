import os
import json
import re

INPUT_DIR = "data/output/data"
OUTPUT_DIR = "data/output/normalized"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def safe_get(obj, key, default=None):
    """Safely get a key from a dict."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return default

def ensure_list(value):
    """Ensure output is always a list."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]

def extract_year(date_string):
    """Extract year from a date string."""
    if not date_string or not isinstance(date_string, str):
        return None
    
    # Try to match 4-digit year
    year_match = re.search(r'\b(19|20)\d{2}\b', date_string)
    if year_match:
        return year_match.group(0)
    
    return None

def normalize_dates(date_list):
    """
    Normalize date values to structured format with full date and year.
    Returns a list of date objects with 'full' and 'year' fields.
    """
    if not date_list:
        return None
    
    normalized_dates = []
    
    for date_val in date_list:
        if not date_val or not isinstance(date_val, str):
            continue
        
        clean_date = date_val.strip()
        if not clean_date:
            continue
        
        # Extract year from the date
        year = extract_year(clean_date)
        
        # Create date object
        date_obj = {
            "full": clean_date,
            "year": year
        }
        
        normalized_dates.append(date_obj)
    
    return normalized_dates if normalized_dates else None

def normalize_rights(rights_list):
    """
    Normalize rights values from 'info:eu-repo/semantics/openAccess' 
    to just 'openAccess', etc. Keeps URLs (like Creative Commons) as-is.
    """
    normalized = []
    valid_rights = {
        'openaccess': 'openAccess',
        'open access': 'openAccess',
        'embargoedaccess': 'embargoedAccess',
        'embargoed access': 'embargoedAccess',
        'restrictedaccess': 'restrictedAccess',
        'restricted access': 'restrictedAccess',
        'closedaccess': 'closedAccess',
        'closed access': 'closedAccess',
        'metadataonlyaccess': 'metadataOnlyAccess',
        'metadata only access': 'metadataOnlyAccess'
    }
    
    for right in rights_list:
        if not right or not isinstance(right, str):
            continue
        
        # Clean the string
        clean_right = right.strip()
        clean_lower = clean_right.lower()
        
        # If it's a URL, keep it as-is
        if clean_right.startswith('http://') or clean_right.startswith('https://'):
            normalized.append(clean_right)
            continue
        
        # Check if it's already a valid value (exact match)
        if clean_right in valid_rights.values():
            normalized.append(clean_right)
            continue
        
        # Try to extract from info:eu-repo/semantics/ pattern
        if 'info:eu-repo/semantics/' in clean_right:
            # Extract everything after the last slash
            extracted = clean_right.split('info:eu-repo/semantics/')[-1].strip()
            # Clean up the extracted value
            extracted_clean = extracted.lower().replace('-', '').replace('_', '').replace(' ', '')
            
            # Try to match with valid rights
            for key, value in valid_rights.items():
                if key.replace(' ', '') == extracted_clean:
                    normalized.append(value)
                    break
            else:
                # If no match, try to format it nicely
                if extracted_clean in ['openaccess', 'open']:
                    normalized.append('openAccess')
                elif extracted_clean in ['embargoedaccess', 'embargoed']:
                    normalized.append('embargoedAccess')
                elif extracted_clean in ['restrictedaccess', 'restricted']:
                    normalized.append('restrictedAccess')
                elif extracted_clean in ['closedaccess', 'closed']:
                    normalized.append('closedAccess')
                else:
                    # Keep the original if we can't categorize it
                    normalized.append(clean_right)
            continue
        
        # Try to find any of the valid rights in the string (case-insensitive)
        found = False
        clean_compare = clean_lower.replace('-', '').replace('_', '').replace(' ', '')
        for key, value in valid_rights.items():
            key_compare = key.replace(' ', '')
            if key_compare in clean_compare:
                normalized.append(value)
                found = True
                break
        
        if not found:
            # Keep the original value if it's not empty
            if clean_right:
                normalized.append(clean_right)
    
    # Remove duplicates while preserving order
    seen = set()
    result = []
    for item in normalized:
        if item not in seen:
            seen.add(item)
            result.append(item)
    
    return result if result else None

def normalize_type(type_value):
    """
    Normalize type values from 'info:eu-repo/semantics/article' to just 'article', etc.
    """
    if not type_value or not isinstance(type_value, str):
        return None
    
    # Clean the string
    clean_type = type_value.strip()
    
    # Common types to extract
    valid_types = [
        'article',
        'bachelorThesis',
        'masterThesis',
        'doctoralThesis',
        'book',
        'bookPart',
        'review',
        'conferenceObject',
        'lecture',
        'workingPaper',
        'preprint',
        'report',
        'annotation',
        'contributionToPeriodical',
        'patent',
        'other'
    ]
    
    # Try to extract the type from URI patterns like info:eu-repo/semantics/article
    if 'info:eu-repo/semantics/' in clean_type:
        extracted = clean_type.split('info:eu-repo/semantics/')[-1]
        return extracted if extracted else clean_type
    
    # Check if it matches any known type (case-insensitive)
    clean_lower = clean_type.lower()
    for vtype in valid_types:
        if vtype.lower() == clean_lower:
            return vtype
    
    # If no pattern matches, return the original value
    return clean_type

def normalize_format(format_list):
    """
    Normalize format values from MIME types to short keys.
    Examples:
      'application/pdf' -> 'pdf'
      'image/jpeg' -> 'jpeg'
      'text/html' -> 'html'
    """
    normalized = []
    
    # Mapping of common MIME types and variations to short keys
    format_mapping = {
        # Documents
        'application/pdf': 'pdf',
        'application/xml': 'xml',
        'text/xml': 'xml',
        'text/plain': 'txt',
        'text/html': 'html',
        'text/css': 'css',
        'application/msword': 'doc',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
        'application/vnd.ms-powerpoint': 'ppt',
        'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'pptx',
        'application/vnd.ms-excel': 'xls',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
        'application/rtf': 'rtf',
        'text/csv': 'csv',
        'application/zip': 'zip',
        'application/x-rar-compressed': 'rar',
        
        # Images
        'image/jpeg': 'jpeg',
        'image/jpg': 'jpeg',
        'image/gif': 'gif',
        'image/png': 'png',
        'image/tiff': 'tiff',
        'image/bmp': 'bmp',
        'image/svg+xml': 'svg',
        
        # Audio
        'audio/aiff': 'aiff',
        'audio/basic': 'au',
        'audio/wav': 'wav',
        'audio/x-wav': 'wav',
        'audio/mpeg': 'mpeg',
        'audio/mp3': 'mpeg',
        
        # Video
        'video/mpeg': 'mpeg',
        'video/quicktime': 'mov',
        
        # OpenDocument
        'application/vnd.oasis.opendocument.text': 'odt',
        'application/vnd.oasis.opendocument.spreadsheet': 'ods',
        'application/vnd.oasis.opendocument.presentation': 'odp',
        'application/vnd.oasis.opendocument.graphics': 'odg',
        
        # Other
        'application/postscript': 'ps',
        'application/x-latex': 'latex',
        'application/x-tex': 'tex',
        'application/x-dvi': 'dvi',
        'application/rdf+xml': 'rdf',
    }
    
    for fmt in format_list:
        if not fmt or not isinstance(fmt, str):
            continue
        
        # Clean the string
        clean_fmt = fmt.strip().lower()
        
        # Check if it's in our mapping
        if clean_fmt in format_mapping:
            normalized.append(format_mapping[clean_fmt])
            continue
        
        # Try to extract from MIME type pattern (type/subtype)
        if '/' in clean_fmt:
            # Extract the part after the slash
            parts = clean_fmt.split('/')
            if len(parts) == 2:
                subtype = parts[1]
                
                # Clean up subtypes like 'vnd.ms-excel' to just 'excel'
                # or 'x-latex' to 'latex'
                if subtype.startswith('x-'):
                    subtype = subtype[2:]
                elif subtype.startswith('vnd.'):
                    # Handle vendor-specific types
                    if 'excel' in subtype:
                        normalized.append('xls')
                        continue
                    elif 'word' in subtype:
                        normalized.append('doc')
                        continue
                    elif 'powerpoint' in subtype:
                        normalized.append('ppt')
                        continue
                    elif 'opendocument' in subtype:
                        # Extract odt, ods, etc from the end
                        if 'text' in subtype:
                            normalized.append('odt')
                        elif 'spreadsheet' in subtype:
                            normalized.append('ods')
                        elif 'presentation' in subtype:
                            normalized.append('odp')
                        elif 'graphics' in subtype or 'drawing' in subtype:
                            normalized.append('odg')
                        continue
                
                # Use the cleaned subtype
                normalized.append(subtype)
                continue
        
        # If it looks like a file extension (no slashes, short), use it as-is
        if '/' not in clean_fmt and len(clean_fmt) <= 10:
            normalized.append(clean_fmt)
            continue
        
        # Otherwise, keep the original value
        normalized.append(fmt.strip())
    
    # Remove duplicates while preserving order
    seen = set()
    result = []
    for item in normalized:
        if item not in seen:
            seen.add(item)
            result.append(item)
    
    return result if result else None

def normalize_subject(subject_list):
    """
    Main classifications (CTI 1-7) are moved to the top of the list.
    """
    # CTI Area classification mapping (1 digit)
    cti_area_mapping = {
        '1': 'CIENCIAS FÍSICO MATEMÁTICAS Y CIENCIAS DE LA TIERRA',
        '2': 'BIOLOGÍA Y QUÍMICA',
        '3': 'MEDICINA Y CIENCIAS DE LA SALUD',
        '4': 'HUMANIDADES Y CIENCIAS DE LA CONDUCTA',
        '5': 'CIENCIAS SOCIALES',
        '6': 'CIENCIAS AGROPECUARIAS Y BIOTECNOLOGÍA',
        '7': 'INGENIERÍA Y TECNOLOGÍA'
    }
    
    # CTI Field classification mapping (2 digits)
    cti_field_mapping = {
        '11': 'LÓGICA', '12': 'MATEMÁTICAS', '21': 'ASTRONOMÍA Y ASTROFÍSICA',
        '22': 'FÍSICA', '25': 'CIENCIAS DE LA TIERRA Y DEL ESPACIO', '23': 'QUÍMICA',
        '24': 'CIENCIAS DE LA VIDA', '32': 'CIENCIAS MÉDICAS', '51': 'ANTROPOLOGÍA',
        '57': 'LINGÜÍSTICA', '58': 'PEDAGOGÍA', '61': 'PSICOLOGÍA',
        '62': 'CIENCIAS DE LAS ARTES Y LAS LETRAS', '71': 'ÉTICA', '72': 'FILOSOFÍA',
        '52': 'DEMOGRAFÍA', '53': 'CIENCIAS ECONÓMICAS', '54': 'GEOGRAFÍA',
        '55': 'HISTORIA', '56': 'CIENCIAS JURÍDICAS Y DERECHO', '59': 'CIENCIA POLÍTICA',
        '63': 'SOCIOLOGÍA', '31': 'CIENCIAS AGRARIAS', '33': 'CIENCIAS TECNOLÓGICAS'
    }
    
    # CTI Discipline classification mapping (4 digits) - principales
    cti_discipline_mapping = {
        '1101': 'APLICACIONES DE LA LÓGICA', '1102': 'LÓGICA DEDUCTIVA', '1103': 'LÓGICA GENERAL',
        '1104': 'LÓGICA INDUCTIVA', '1105': 'METODOLOGÍA', '1199': 'OTRAS ESPECIALIDADES RELATIVAS A LA LÓGICA',
        '1201': 'ÁLGEBRA', '1202': 'ANÁLISIS Y ANÁLISIS FUNCIONAL', '1203': 'CIENCIA DE LOS ORDENADORES',
        '1204': 'GEOMETRÍA', '1205': 'TEORÍA DE NÚMEROS', '1206': 'ANÁLISIS NUMÉRICO',
        '1207': 'INVESTIGACIÓN OPERATIVA', '1208': 'PROBABILIDAD', '1209': 'ESTADÍSTICA',
        '1210': 'TOPOLOGÍA', '1299': 'OTRAS ESPECIALIDADES MATEMÁTICAS',
        '2101': 'COSMOLOGÍA Y COSMOGONIA', '2102': 'MEDIO INTERPLANETARIO', '2103': 'ASTRONOMÍA ÓPTICA',
        '2104': 'PLANETOLOGÍA', '2105': 'RADIOASTRONOMÍA', '2106': 'SISTEMA SOLAR',
        '2199': 'OTRAS ESPECIALIDADES ASTRONÓMICAS',
        '2201': 'ACÚSTICA', '2202': 'ELECTROMAGNETISMO', '2203': 'ELECTRÓNICA',
        '2204': 'FÍSICA DE FLUÍDOS', '2205': 'MECÁNICA', '2206': 'FÍSICA MOLECULAR',
        '2207': 'FÍSICA ATÓMICA Y NUCLEAR', '2208': 'NUCLEÓNICA', '2209': 'ÓPTICA',
        '2210': 'QUÍMICA FÍSICA', '2211': 'FÍSICA DEL ESTADO SÓLIDO', '2212': 'FÍSICA TEÓRICA',
        '2213': 'TERMODINÁMICA', '2214': 'UNIDADES Y CONSTANTES', '2290': 'FÍSICA DE ALTAS ENERGÍAS',
        '2299': 'OTRAS ESPECIALIDADES FÍSICAS',
        '2501': 'CIENCIAS DE LA ATMÓSFERA', '2502': 'CLIMATOLOGÍA', '2503': 'GEOQUÍMICA',
        '2504': 'GEODESIA', '2505': 'GEOGRAFÍA', '2506': 'GEOLOGÍA', '2507': 'GEOFÍSICA',
        '2508': 'HIDROLOGÍA', '2509': 'METEOROLOGÍA', '2510': 'OCEANOGRAFÍA',
        '2511': 'CIENCIAS DEL SUELO (EDAFOLOGÍA)', '2512': 'CIENCIAS DEL ESPACIO',
        '2599': 'OTRAS ESPECIALIDADES DE LA TIERRA, ESPACIO O ENTORNO',
        '2301': 'QUÍMICA ANALÍTICA', '2302': 'BIOQUÍMICA', '2303': 'QUÍMICA INORGÁNICA',
        '2304': 'QUÍMICA MACROMOLECULAR', '2305': 'QUÍMICA NUCLEAR', '2306': 'QUÍMICA ORGÁNICA',
        '2307': 'QUÍMICA FÍSICA', '2390': 'QUÍMICA FARMACÉUTICA', '2399': 'OTRAS ESPECIALIDADES QUÍMICAS',
        '2401': 'BIOLOGÍA ANIMAL (ZOOLOGÍA)', '2402': 'ANTROPOLOGÍA (FÍSICA)', '2403': 'BIOQUÍMICA',
        '2404': 'BIOMATEMÁTICAS', '2405': 'BIOMETRÍA', '2406': 'BIOFÍSICA', '2407': 'BIOLOGÍA CELULAR',
        '2408': 'ETOLOGÍA', '2409': 'GENÉTICA', '2410': 'BIOLOGÍA HUMANA', '2411': 'FISIOLOGÍA HUMANA',
        '2412': 'INMUNOLOGÍA', '2413': 'BIOLOGÍA DE INSECTOS (ENTOMOLOGÍA)', '2414': 'MICROBIOLOGÍA',
        '2415': 'BIOLOGÍA MOLECULAR', '2416': 'PALEONTOLOGÍA', '2417': 'BIOLOGÍA VEGETAL (BOTÁNICA)',
        '2418': 'RADIOBIOLOGÍA', '2419': 'SIMBIOSIS', '2420': 'VIROLOGÍA',
        '2490': 'NEUROCIENCIAS', '2499': 'OTRAS ESPECIALIDADES DE LA BIOLOGÍA',
        '3201': 'CIENCIAS CLÍNICAS', '3202': 'EPIDEMIOLOGÍA', '3203': 'MEDICINA FORENSE',
        '3204': 'MEDICINA DEL TRABAJO', '3205': 'MEDICINA INTERNA', '3206': 'CIENCIAS DE LA NUTRICIÓN',
        '3207': 'PATOLOGÍA', '3208': 'FARMACODINÁMICA', '3209': 'FARMACOLOGÍA',
        '3210': 'MEDICINA PREVENTIVA', '3211': 'PSIQUIATRÍA', '3212': 'SALUD PÚBLICA',
        '3213': 'CIRUGÍA', '3214': 'TOXICOLOGÍA', '3299': 'OTRAS ESPECIALIDADES MÉDICAS',
        '5101': 'ANTROPOLOGÍA CULTURAL', '5102': 'ETNOGRAFÍA Y ETNOLOGÍA', '5103': 'ANTROPOLOGÍA SOCIAL',
        '5199': 'OTRAS ESPECIALIDADES ANTROPOLÓGICAS',
        '5701': 'LINGÜÍSTICA APLICADA', '5702': 'LINGÜÍSTICA DIACRÓNICA', '5703': 'GEOGRAFÍA LINGÜÍSTICA',
        '5704': 'TEORÍA LINGÜÍSTICA', '5705': 'LINGÜÍSTICA SINCRÓNICA', '5799': 'OTRAS ESPECIALIDADES LINGÜÍSTICAS',
        '5801': 'TEORÍA Y MÉTODOS EDUCATIVOS', '5802': 'ORGANIZACIÓN Y PLANIFICACIÓN DE LA EDUCACIÓN',
        '5803': 'PREPARACIÓN Y EMPLEO DE PROFESORES', '5899': 'OTRAS ESPECIALIDADES PEDAGÓGICAS',
        '6101': 'PATOLOGÍA', '6102': 'PSICOLOGÍA DEL NIÑO Y DEL ADOLESCENTE', '6103': 'ASESORAMIENTO Y ORIENTACIÓN',
        '6104': 'PSICOPEDAGOGÍA', '6105': 'EVALUACIÓN Y DIAGNÓSTICO EN PSICOLOGÍA', '6106': 'PSICOLOGÍA EXPERIMENTAL',
        '6107': 'PSICOLOGÍA GENERAL', '6108': 'PSICOLOGÍA DE LA VEJEZ', '6109': 'PSICOLOGÍA INDUSTRIAL',
        '6110': 'PARAPSICOLOGÍA', '6111': 'PERSONALIDAD', '6112': 'ESTUDIO PSICOLÓGICO DE TEMAS SOCIALES',
        '6113': 'PSICOFARMACOLOGÍA', '6114': 'PSICOLOGÍA SOCIAL', '6199': 'OTRAS ESPECIALIDADES PSICOLÓGICAS',
        '6201': 'ARQUITECTURA', '6202': 'TEORÍA, ANÁLISIS Y CRÍTICA LITERARIAS',
        '6203': 'TEORÍA, ANÁLISIS Y CRÍTICA DE LAS BELLAS ARTES', '6299': 'OTRAS ESPECIALIDADES ARTÍSTICAS',
        '7101': 'ÉTICA CLÁSICA', '7102': 'ÉTICA DE INDIVIDUOS', '7103': 'ÉTICA DE GRUPO',
        '7104': 'LA ÉTICA EN PERSPECTIVA', '7199': 'OTRAS ESPECIALIDADES RELACIONADAS CON LA ÉTICA',
        '7201': 'FILOSOFÍA DEL CONOCIMIENTO', '7202': 'ANTROPOLOGÍA FILOSÓFICA', '7203': 'FILOSOFÍA GENERAL',
        '7204': 'SISTEMAS FILOSÓFICOS', '7205': 'FILOSOFÍA DE LA CIENCIA', '7206': 'FILOSOFÍA DE LA NATURALEZA',
        '7207': 'FILOSOFÍA SOCIAL', '7208': 'DOCTRINAS FILOSÓFICAS', '7209': 'OTRAS ESPECIALIDADES FILOSÓFICAS',
        '5201': 'FERTILIDAD', '5202': 'DEMOGRAFÍA GENERAL', '5203': 'DEMOGRAFÍA GEOGRÁFICA',
        '5204': 'DEMOGRAFÍA HISTÓRICA', '5205': 'MORTALIDAD', '5206': 'CARACTERÍSTICAS DE LA POBLACIÓN',
        '5207': 'TAMAÑO DE LA POBLACIÓN Y EVOLUCIÓN DEMOGRÁFICA', '5299': 'OTRAS ESPECIALIDADES DEMOGRÁFICAS',
        '5301': 'POLÍTICA FISCAL Y HACIENDA PUBLICA NACIONALES', '5302': 'ECONOMETRÍA', '5303': 'CONTABILIDAD ECONÓMICA',
        '5304': 'ACTIVIDAD ECONÓMICA', '5305': 'SISTEMAS ECONÓMICOS', '5306': 'ECONOMÍA DEL CAMBIO TECNOLÓGICO',
        '5307': 'TEORÍA ECONÓMICA', '5308': 'ECONOMÍA GENERAL', '5309': 'ORGANIZACIÓN INDUSTRIAL Y POLÍTICA PÚBLICA',
        '5310': 'ECONOMÍA INTERNACIONAL', '5311': 'ORGANIZACIÓN Y DIRECCIÓN DE EMPRESAS', '5312': 'ECONOMÍA SECTORIAL',
        '5399': 'OTRAS ESPECIALIDADES ECONÓMICAS',
        '5401': 'GEOGRAFÍA ECONÓMICA', '5402': 'GEOGRAFÍA HISTÓRICA', '5403': 'GEOGRAFÍA HUMANA',
        '5404': 'GEOGRAFÍA REGIONAL', '5499': 'OTRAS ESPECIALIDADES GEOGRÁFICAS',
        '5502': 'HISTORIA GENERAL', '5503': 'HISTORIA DE PAÍSES', '5504': 'HISTORIA POR ÉPOCAS',
        '5505': 'CIENCIAS AUXILIARES DE LA HISTORIA', '5506': 'HISTORIA POR ESPECIALIDADES',
        '5599': 'OTRAS ESPECIALIDADES HISTÓRICAS',
        '5601': 'DERECHO CANÓNICO', '5602': 'TEORÍA Y MÉTODOS GENERALES', '5603': 'DERECHO INTERNACIONAL',
        '5604': 'ORGANIZACIÓN JURÍDICA', '5605': 'DERECHO Y LEGISLACIÓN NACIONALES', '5699': 'OTRAS ESPECIALIDADES JURÍDICAS',
        '5901': 'RELACIONES INTERNACIONALES', '5902': 'CIENCIAS POLÍTICAS', '5903': 'IDEOLOGÍAS POLÍTICAS',
        '5904': 'INSTITUCIONES POLÍTICAS', '5905': 'VIDA POLÍTICA', '5906': 'SOCIOLOGÍA POLÍTICA',
        '5907': 'SISTEMAS POLÍTICOS', '5908': 'TEORÍA POLÍTICA', '5909': 'ADMINISTRACIÓN PÚBLICA',
        '5910': 'OPINIÓN PÚBLICA', '5999': 'OTRAS ESPECIALIDADES POLÍTICAS',
        '6301': 'SOCIOLOGÍA CULTURAL', '6302': 'SOCIOLOGÍA EXPERIMENTAL', '6303': 'SOCIOLOGÍA GENERAL',
        '6304': 'PROBLEMAS INTERNACIONALES', '6305': 'SOCIOLOGÍA MATEMÁTICA', '6306': 'SOCIOLOGÍA DEL TRABAJO',
        '6307': 'CAMBIO Y DESARROLLO SOCIAL', '6308': 'COMUNICACIONES SOCIALES', '6309': 'GRUPOS SOCIALES',
        '6310': 'PROBLEMAS SOCIALES', '6311': 'SOCIOLOGÍA DE LOS ASENTAMIENTOS HUMANOS', '6399': 'OTRAS ESPECIALIDADES SOCIOLÓGICAS',
        '3101': 'AGROQUÍMICA', '3103': 'AGRONOMÍA', '3104': 'PRODUCCIÓN ANIMAL',
        '3105': 'PECES Y FAUNA SILVESTRE', '3106': 'CIENCIA FORESTAL', '3107': 'HORTICULTURA',
        '3108': 'FITOPATOLOGÍA', '3109': 'CIENCIAS VETERINARIAS', '3199': 'OTRAS ESPECIALIDADES AGRARIAS',
        '3301': 'INGENIERÍA Y TECNOLOGÍA AERONÁUTICAS', '3302': 'TECNOLOGÍA BIOQUÍMICA',
        '3303': 'INGENIERÍA Y TECNOLOGÍA QUÍMICAS', '3304': 'TECNOLOGÍA DE LOS ORDENADORES',
        '3305': 'TECNOLOGÍA DE LA CONSTRUCCIÓN', '3306': 'INGENIERÍA Y TECNOLOGÍA ELÉCTRICAS',
        '3307': 'TECNOLOGÍA ELECTRÓNICA', '3308': 'INGENIERÍA Y TECNOLOGÍA DEL MEDIO AMBIENTE',
        '3309': 'TECNOLOGÍA DE LOS ALIMENTOS', '3310': 'TECNOLOGÍA INDUSTRIAL',
        '3311': 'TECNOLOGÍA DE LA INSTRUMENTACIÓN', '3312': 'TECNOLOGÍA DE MATERIALES',
        '3313': 'TECNOLOGÍA E INGENIERÍA MECÁNICAS', '3314': 'TECNOLOGÍA MÉDICA',
        '3315': 'TECNOLOGÍA METALÚRGICA', '3317': 'TECNOLOGÍA DE VEHÍCULOS DE MOTOR',
        '3318': 'TECNOLOGÍA MINERA', '3319': 'TECNOLOGÍA NAVAL', '3320': 'TECNOLOGÍA NUCLEAR',
        '3321': 'TECNOLOGÍA DEL CARBÓN Y DEL PETRÓLEO', '3322': 'TECNOLOGÍA ENERGÉTICA',
        '3323': 'TECNOLOGÍA DE LOS FERROCARRILES', '3324': 'TECNOLOGÍA DEL ESPACIO',
        '3325': 'TECNOLOGÍA DE LAS TELECOMUNICACIONES', '3326': 'TECNOLOGÍA TEXTIL',
        '3327': 'TECNOLOGÍA DE LOS SISTEMAS DE TRANSPORTE', '3328': 'PROCESOS TECNOLÓGICOS',
        '3329': 'PLANIFICACIÓN URBANA', '3399': 'OTRAS ESPECIALIDADES TECNOLÓGICAS'
    }
    
    # Set of main classification values (areas 1-7)
    main_classifications = set(cti_area_mapping.values())
    
    main_subjects = []
    other_subjects = []
    
    for subj in subject_list:
        if not subj or not isinstance(subj, str):
            continue
        
        clean_subj = subj.strip()
        is_main_classification = False
        
        # Check if it's a CTI classification URI
        if 'info:eu-repo/classification/cti/' in clean_subj:
            # Extract the number/code after cti/
            cti_match = re.search(r'info:eu-repo/classification/cti/(\d+)', clean_subj)
            if cti_match:
                cti_code = cti_match.group(1)
                
                # Check if it's a discipline (4 digits)
                if len(cti_code) == 4 and cti_code in cti_discipline_mapping:
                    other_subjects.append(cti_discipline_mapping[cti_code])
                    continue
                # Check if it's a field (2 digits)
                elif len(cti_code) == 2 and cti_code in cti_field_mapping:
                    other_subjects.append(cti_field_mapping[cti_code])
                    continue
                # Check if it's an area (1 digit)
                elif len(cti_code) == 1 and cti_code in cti_area_mapping:
                    main_subjects.append(cti_area_mapping[cti_code])
                    is_main_classification = True
                    continue
        
        # Check if it's already a main classification value
        if clean_subj in main_classifications:
            main_subjects.append(clean_subj)
            is_main_classification = True
            continue
        
        # Check if it's a librunam classification URI
        if 'info:eu-repo/classification/librunam/' in clean_subj:
            # Extract everything after 'librunam/'
            extracted = clean_subj.split('info:eu-repo/classification/librunam/')[-1].strip()
            if extracted:
                other_subjects.append(extracted)
                continue
        
        # Check for other info:eu-repo/classification/ patterns
        if 'info:eu-repo/classification/' in clean_subj:
            # Extract everything after the last slash
            parts = clean_subj.split('/')
            if len(parts) > 0:
                extracted = parts[-1].strip()
                if extracted:
                    other_subjects.append(extracted)
                    continue
        
        # If no pattern matches, add to other subjects
        if clean_subj:
            other_subjects.append(clean_subj)
    
    # Remove duplicates while preserving order in each group
    seen_main = set()
    unique_main = []
    for item in main_subjects:
        if item not in seen_main:
            seen_main.add(item)
            unique_main.append(item)
    
    seen_other = set()
    unique_other = []
    for item in other_subjects:
        if item not in seen_other:
            seen_other.add(item)
            unique_other.append(item)
    
    # Combine: main classifications first, then other subjects
    result = unique_main + unique_other
    
    return result if result else None

def normalize_language(lang_value):
    """
    Normalize language values to ISO 639-3 codes (3-letter codes).
    Examples:
      'eng' -> 'eng'
      'en' -> 'eng'
      'English' -> 'eng'
      'español' -> 'spa'
    """
    if not lang_value or not isinstance(lang_value, str):
        return None
    
    # Clean the string
    clean_lang = lang_value.strip().lower()
    
    # Mapping from ISO 639-1 (2-letter) to ISO 639-3 (3-letter)
    iso_639_1_to_3 = {
        'af': 'afr', 'sq': 'sqi', 'am': 'amh', 'ar': 'ara', 'hy': 'hye',
        'as': 'asm', 'ba': 'bak', 'eu': 'eus', 'be': 'bel', 'bn': 'ben',
        'br': 'bre', 'bg': 'bul', 'ca': 'cat', 'co': 'cos', 'hr': 'hrv',
        'cs': 'ces', 'da': 'dan', 'nl': 'nld', 'en': 'eng', 'et': 'est',
        'fo': 'fao', 'fi': 'fin', 'fr': 'fra', 'gl': 'glg', 'ka': 'kat',
        'de': 'deu', 'gu': 'guj', 'he': 'heb', 'hi': 'hin', 'hu': 'hun',
        'is': 'isl', 'ig': 'ibo', 'id': 'ind', 'ga': 'gle', 'it': 'ita',
        'ja': 'jpn', 'kn': 'kan', 'kk': 'kaz', 'rw': 'kin', 'ko': 'kor',
        'lo': 'lao', 'lv': 'lav', 'lt': 'lit', 'lb': 'ltz', 'ml': 'mal',
        'mt': 'mlt', 'mi': 'mri', 'mr': 'mar', 'fa': 'fas', 'pl': 'pol',
        'pt': 'por', 'qu': 'que', 'ro': 'ron', 'rm': 'roh', 'ru': 'rus',
        'gd': 'gla', 'si': 'sin', 'sk': 'slk', 'sl': 'slv', 'es': 'spa',
        'sv': 'swe', 'ta': 'tam', 'tt': 'tat', 'te': 'tel', 'th': 'tha',
        'bo': 'bod', 'tr': 'tur', 'tk': 'tuk', 'uk': 'ukr', 'ur': 'urd',
        'vi': 'vie', 'cy': 'cym', 'wo': 'wol', 'yi': 'yor', 'sa': 'san',
        'zh': 'zho'
    }
    
    # Valid ISO 639-3 codes
    valid_iso_639_3 = {
        'afr', 'sqi', 'amh', 'ara', 'hye', 'asm', 'bak', 'eus', 'bel', 'ben',
        'bre', 'bul', 'cat', 'cos', 'hrv', 'ces', 'dan', 'prs', 'nld', 'eng',
        'est', 'fao', 'fil', 'fin', 'fra', 'glg', 'kat', 'deu', 'guj', 'heb',
        'hin', 'hun', 'isl', 'ibo', 'ind', 'gle', 'ita', 'jpn', 'kan', 'kaz',
        'kin', 'kor', 'lao', 'lav', 'lit', 'dsb', 'ltz', 'mal', 'mlt', 'mri',
        'arn', 'mar', 'moh', 'fas', 'pol', 'por', 'que', 'ron', 'roh', 'rus',
        'gla', 'sin', 'slk', 'slv', 'spa', 'swe', 'syr', 'tam', 'tat', 'tel',
        'tha', 'bod', 'tur', 'tuk', 'ukr', 'hsb', 'urd', 'vie', 'cym', 'wol',
        'sah', 'yor', 'san', 'zho'
    }
    
    # Language name to ISO 639-3 mapping
    name_to_iso_639_3 = {
        'afrikaans': 'afr', 'albanian': 'sqi', 'albanés': 'sqi', 'amharic': 'amh', 'amárico': 'amh',
        'arabic': 'ara', 'árabe': 'ara', 'armenian': 'hye', 'armenio': 'hye', 'assamese': 'asm', 'asamés': 'asm',
        'bashkir': 'bak', 'basque': 'eus', 'vasco': 'eus', 'belarusian': 'bel', 'bielorruso': 'bel',
        'bengali': 'ben', 'bengalí': 'ben', 'breton': 'bre', 'bretón': 'bre', 'bulgarian': 'bul', 'búlgaro': 'bul',
        'catalan': 'cat', 'catalán': 'cat', 'corsican': 'cos', 'corsa': 'cos', 'croatian': 'hrv', 'croata': 'hrv',
        'czech': 'ces', 'checo': 'ces', 'danish': 'dan', 'danés': 'dan', 'dari': 'prs',
        'dutch': 'nld', 'holandés': 'nld', 'english': 'eng', 'inglés': 'eng', 'estonian': 'est', 'estonio': 'est',
        'faroese': 'fao', 'feroés': 'fao', 'filipino': 'fil', 'finnish': 'fin', 'finlandés': 'fin',
        'french': 'fra', 'francés': 'fra', 'galician': 'glg', 'gallego': 'glg', 'georgian': 'kat', 'georgiano': 'kat',
        'german': 'deu', 'alemán': 'deu', 'gujarati': 'guj', 'hebrew': 'heb', 'hebreo': 'heb',
        'hindi': 'hin', 'hungarian': 'hun', 'húngaro': 'hun', 'icelandic': 'isl', 'islandés': 'isl',
        'igbo': 'ibo', 'indonesian': 'ind', 'indonesio': 'ind', 'irish': 'gle', 'irlandés': 'gle',
        'italian': 'ita', 'italiano': 'ita', 'japanese': 'jpn', 'japonés': 'jpn', 'kannada': 'kan',
        'kazakh': 'kaz', 'kazajo': 'kaz', 'kinyarwanda': 'kin', 'korean': 'kor', 'coreano': 'kor',
        'lao': 'lao', 'latvian': 'lav', 'letón': 'lav', 'lithuanian': 'lit', 'lituano': 'lit',
        'lower sorbian': 'dsb', 'bajo sorbio': 'dsb', 'luxembourgish': 'ltz', 'luxemburgués': 'ltz',
        'malayalam': 'mal', 'maltese': 'mlt', 'maltés': 'mlt', 'maori': 'mri', 'maorí': 'mri',
        'mapudungun': 'arn', 'marathi': 'mar', 'mohawk': 'moh', 'persian': 'fas', 'persa': 'fas',
        'polish': 'pol', 'polaco': 'pol', 'portuguese': 'por', 'portugués': 'por', 'quechua': 'que',
        'romanian': 'ron', 'rumano': 'ron', 'romansh': 'roh', 'romanche': 'roh', 'russian': 'rus', 'ruso': 'rus',
        'scottish gaelic': 'gla', 'gaélico escocés': 'gla', 'sinhala': 'sin', 'cingalés': 'sin',
        'slovak': 'slk', 'eslovaco': 'slk', 'slovenian': 'slv', 'esloveno': 'slv',
        'spanish': 'spa', 'español': 'spa', 'swedish': 'swe', 'sueco': 'swe', 'syriac': 'syr', 'siríaco': 'syr',
        'tamil': 'tam', 'tatar': 'tat', 'tártaro': 'tat', 'telugu': 'tel', 'thai': 'tha', 'tailandés': 'tha',
        'tibetan': 'bod', 'tibetano': 'bod', 'turkish': 'tur', 'turco': 'tur', 'turkmen': 'tuk', 'turcomano': 'tuk',
        'ukrainian': 'ukr', 'ucraniano': 'ukr', 'upper sorbian': 'hsb', 'sorbian superior': 'hsb',
        'urdu': 'urd', 'vietnamese': 'vie', 'vietnamita': 'vie', 'welsh': 'cym', 'galés': 'cym',
        'wolof': 'wol', 'yakut': 'sah', 'yakuto': 'sah', 'yoruba': 'yor', 'sanskrit': 'san', 'sanscrito': 'san',
        'chinese': 'zho', 'chino': 'zho'
    }
    
    # Check if it's already a valid ISO 639-3 code
    if clean_lang in valid_iso_639_3:
        return clean_lang
    
    # Check if it's an ISO 639-1 code (2 letters)
    if len(clean_lang) == 2 and clean_lang in iso_639_1_to_3:
        return iso_639_1_to_3[clean_lang]
    
    # Check if it's a language name
    if clean_lang in name_to_iso_639_3:
        return name_to_iso_639_3[clean_lang]
    
    # Return the original value if we can't normalize it
    return lang_value.strip()

def normalize_record(record, repository_name):
    """Extract and normalize a single OAI-PMH record safely."""
    header = safe_get(record, "header", {})
    metadata = safe_get(record, "metadata", {})
    dc = safe_get(metadata, "oai_dc:dc", {})
    
    # Extract metadata fields safely
    title = safe_get(dc, "dc:title")
    creator = safe_get(dc, "dc:creator")
    description = safe_get(dc, "dc:description")
    subject = ensure_list(safe_get(dc, "dc:subject"))
    rights = ensure_list(safe_get(dc, "dc:rights"))
    fmt = ensure_list(safe_get(dc, "dc:format"))
    type_ = safe_get(dc, "dc:type")
    lang = safe_get(dc, "dc:language")
    date = ensure_list(safe_get(dc, "dc:date"))
    identifier = ensure_list(safe_get(dc, "dc:identifier"))
    
    # Normalize subject values (includes CTI classification decoding)
    subject = normalize_subject(subject)
    
    # Normalize rights values
    rights = normalize_rights(rights)
    
    # Normalize type value
    type_ = normalize_type(type_)
    
    # Normalize format values
    fmt = normalize_format(fmt)
    
    # Normalize dates
    normalized_dates = normalize_dates(date)
    
    # Build normalized object
    normalized = {
        "id": safe_get(header, "identifier"),
        "repository": repository_name,
        "identifier": identifier if identifier else None,
        "datestamp": safe_get(header, "datestamp"),
        "setSpec": ensure_list(safe_get(header, "setSpec")) or None,
        "title": title if title else None,
        "creator": creator if creator else None,
        "date": normalized_dates,
        "description": description if description else None,
        "subject": subject if subject else None,
        "rights": rights if rights else None,
        "format": fmt if fmt else None,
        "type": type_ if type_ else None,
        "language": lang if lang else None
    }
    
    return normalized

def process_file(path):
    """Load OAI-PMH JSON, extract records, and write normalized output safely."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        print(f"[Error] reading file: {path}")
        print("   →", e)
        return
    
    root = safe_get(raw, "OAI-PMH", {})
    list_records = safe_get(root, "ListRecords", {})
    records = ensure_list(safe_get(list_records, "record"))
    
    repository_name = os.path.basename(path).replace(".json", "")
    normalized_list = []
    
    for index, record in enumerate(records):
        try:
            normalized = normalize_record(record, repository_name)
            normalized_list.append(normalized)
        except Exception as e:
            print(f" [Error] processing record #{index} in file: {path}")
            print("   →", e)
            continue
    
    output_path = os.path.join(
        OUTPUT_DIR,
        os.path.basename(path).replace(".json", "_normalized.json")
    )
    
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(normalized_list, f, ensure_ascii=False, indent=2)
        print(f"[OK] Normalized: {output_path} ({len(normalized_list)} records)")
    except Exception as e:
        print(f" [Error] writing output file: {output_path}")
        print("   →", e)

def process_all_files():
    print("=" * 60)
    print("Iniciando normalización de archivos OAI-PMH")
    print("=" * 60)
    
    file_count = 0
    for root, _, files in os.walk(INPUT_DIR):
        for file in files:
            if file.endswith(".json"):
                file_count += 1
                full_path = os.path.join(root, file)
                print(f"\n[{file_count}] Procesando: {file}")
                process_file(full_path)
    
    print("\n" + "=" * 60)
    print(f" Proceso completado: {file_count} archivos procesados")
    print("=" * 60)

if __name__ == "__main__":
    process_all_files()