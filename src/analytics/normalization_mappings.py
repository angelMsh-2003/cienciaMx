# Mappings for data normalization

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

# CTI Discipline classification mapping (4 digits) - main
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
    'Español': 'spa', 'spanish': 'spa', 'español': 'spa', 'castellano': 'spa', 'swedish': 'swe', 'sueco': 'swe', 'syriac': 'syr', 'siríaco': 'syr',
    'tamil': 'tam', 'tatar': 'tat', 'tártaro': 'tat', 'telugu': 'tel', 'thai': 'tha', 'tailandés': 'tha',
    'tibetan': 'bod', 'tibetano': 'bod', 'turkish': 'tur', 'turco': 'tur', 'turkmen': 'tuk', 'turcomano': 'tuk',
    'ukrainian': 'ukr', 'ucraniano': 'ukr', 'upper sorbian': 'hsb', 'sorbian superior': 'hsb',
    'urdu': 'urd', 'vietnamese': 'vie', 'vietnamita': 'vie', 'welsh': 'cym', 'galés': 'cym',
    'wolof': 'wol', 'yakut': 'sah', 'yakuto': 'sah', 'yoruba': 'yor', 'sanskrit': 'san', 'sanscrito': 'san',
    'chinese': 'zho', 'chino': 'zho'
}

# Mapping of common MIME types and variations to short keys
# Strict format mapping keys (user provided)
valid_formats = {
    'unknown', 'pdf', 'xml', 'txt', 'html', 'css', 'doc', 'docx', 'ppt', 'pptx',
    'xls', 'xlsx', 'marc', 'jpeg', 'gif', 'png', 'tiff', 'aiff', 'au', 'wav',
    'mpeg', 'rtf', 'vsd', 'fm', 'bmp', 'psd', 'ps', 'mov', 'mpp', 'ma', 'latex',
    'tex', 'dvi', 'sgml', 'wpd', 'ra', 'pcd', 'odt', 'ott', 'oth', 'odm', 'odg',
    'otg', 'odp', 'otp', 'ods', 'ots', 'odc', 'odf', 'odb', 'odi', 'oxt', 'sxw',
    'stw', 'sxc', 'stc', 'sxd', 'std', 'sxi', 'sti', 'sxg', 'sxm', 'sdw', 'sgl',
    'sdc', 'sda', 'sdd', 'sdp', 'smf', 'sds', 'sdm', 'rdf', 'csv', 'rar', 'zip',
    'svg'
}

# Mapping common mime types to these keys
format_mapping = {
    'application/pdf': 'pdf',
    'text/xml': 'xml', 'application/xml': 'xml',
    'text/plain': 'txt',
    'text/html': 'html',
    'text/css': 'css',
    'application/msword': 'doc',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
    'application/vnd.ms-powerpoint': 'ppt',
    'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'pptx',
    'application/vnd.ms-excel': 'xls',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
    'image/jpeg': 'jpeg', 'image/jpg': 'jpeg',
    'image/gif': 'gif',
    'image/png': 'png',
    'image/tiff': 'tiff',
    'audio/mpeg': 'mpeg', 'video/mpeg': 'mpeg',
    'application/zip': 'zip',
    'application/x-rar-compressed': 'rar'
}

valid_rights = {
    'openaccess': 'openAccess',
    'open access': 'openAccess',
    'embargoedaccess': 'embargoedAccess',
    'embargoed access': 'embargoedAccess',
    'restrictedaccess': 'restrictedAccess',
    'restricted access': 'restrictedAccess',
    'acceso abierto': 'openAccess',
    'accesoabierto': 'openAccess',
    'acceso restringido': 'restrictedAccess',
    'acceso cerrado': 'closedAccess',
    'acceso embargado': 'embargoedAccess',
    'closedaccess': 'closedAccess',
    'closed access': 'closedAccess',
    'metadataonlyaccess': 'metadataOnlyAccess',
    'metadata only access': 'metadataOnlyAccess'
}

# User defined type mapping: Clave -> descCorta
type_mapping = {
    'annotation': 'Anotación',
    'article': 'Artículo',
    'bookPart': 'Capítulo de libro',
    'conferenceObject': 'Objeto de congreso',
    'contributionToPeriodical': 'Contribución a publicación periódica',
    'conferenceContribution': 'Objeto de congreso no publicado',
    'technicalDocumentation': 'Documentación técnica',
    'workingPaper': 'Documento de trabajo',
    'lecture': 'Conferencia',
    'book': 'Libro',
    'conferenceProceedings': 'Memoria de congreso',
    'reportPart': 'Parte de reporte',
    'patent': 'Patente',
    'conferencePaper': 'Ítem publicado en memoria de congreso',
    'conferencePoster': 'Póster de congreso',
    'preprint': 'Preimpreso',
    'researchProposal': 'Protocolo de investigación',
    'report': 'Reporte',
    'review': 'Reseña crítica',
    'doctoralThesis': 'Tesis de doctorado',
    'bachelorThesis': 'Tesis de licenciatura',
    'masterThesis': 'Tesis de maestría',
    'doctoralDegreeWork': 'Trabajo de grado, doctorado',
    'masterDegreeWork': 'Trabajo de grado, maestría',
    'academicSpecialization': 'Trabajo terminal, especialidad',
    'bachelorDegreeWork': 'Trabajo de grado, licenciatura',
    'Event': 'Evento'
}

# Filename cleaning replacements
repository_name_replacements = {
    "Comunicaci n": "Comunicación",
    "P blica": "Pública",
    "M xico": "México",
    "Tecnol gico": "Tecnológico",
    "Aut noma": "Autónoma",
    "Pol tica": "Política",
    "Geot rmicos": "Geotérmicos",
    "Bibliotecol gicas": "Bibliotecológicas",
    "Informaci n": "Información",
    "Matem ticas": "Matemáticas",
    "Gen mico": "Genómico",
    "Qu mica": "Química",
    "Sismol gico": "Sismológico",
    "Ecol gicos": "Ecológicos",
    "Ciencia y Tecnolog a": "Ciencia y Tecnología",
    "Concentraci n": "Concentración",
    "Cient ficos": "Científicos",
    "Cient fica": "Científica",
    "Acad mica": "Académica"
}

# Access terms priority for rights sorting
access_terms_priority = ['openAccess', 'embargoedAccess', 'restrictedAccess', 'closedAccess', 'metadataOnlyAccess']
