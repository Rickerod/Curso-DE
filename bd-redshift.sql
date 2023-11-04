--Datos a guardar en la base de datos
CREATE TABLE sfparevalo_coderhouse.earthquake (
    id INTEGER primary key,
    state_earthquake INTEGER,
    utc_time TIMESTAMP,
    reference VARCHAR(255),
    magnitude DECIMAL(5, 2),
    scale_earthquake VARCHAR(5),
    latitude DECIMAL(8, 4),
    longitude DECIMAL(8, 4),
    depth_earthquake DECIMAL(8, 2)
) DISTSTYLE ALL SORTKEY (utc_time);