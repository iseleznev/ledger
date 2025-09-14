-- liquibase formatted sql

-- changeset Igor_Seleznyov:20250825_002_create_function_uuid7generate
-- comment: Create UUID v7 generation functions
CREATE OR REPLACE FUNCTION generate_uuid_v7()
RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
unix_ts_ms BIGINT;
    time_high INTEGER;
    time_mid INTEGER;
    time_low INTEGER;
    clock_seq_and_reserved INTEGER;
    clock_seq_low INTEGER;
    node_high INTEGER;
    node_low BIGINT;
    uuid_str TEXT;
BEGIN
    -- Получаем текущее время в миллисекундах
    unix_ts_ms := EXTRACT(epoch FROM clock_timestamp()) * 1000;

    -- Старшие 32 бита временной метки
    time_high := (unix_ts_ms >> 16)::INTEGER;

    -- Средние 16 бит временной метки
    time_mid := (unix_ts_ms & x'FFFF'::BIGINT)::INTEGER;

    -- Младшие 12 бит случайные + 4 бита версии (0111 для v7)
    time_low := ((random() * 4095)::INTEGER) | (7 << 12);

    -- Clock sequence (14 бит) + variant (10xx для RFC 4122)
    clock_seq_and_reserved := ((random() * 16383)::INTEGER) | (2 << 14);

    -- 8 бит случайных данных
    clock_seq_low := (random() * 255)::INTEGER;

    -- Последние 48 бит - случайные данные
    node_high := (random() * 65535)::INTEGER;
    node_low := (random() * 4294967295)::BIGINT;

    -- Формируем UUID строку
    uuid_str := format(
        '%08X-%04X-%04X-%04X-%04X%08X',
        time_high,
        time_mid,
        time_low,
        clock_seq_and_reserved,
        node_high,
        node_low
    );

RETURN uuid_str::UUID;
END;
$$;

-- Микросекундная версия для предотвращения коллизий
CREATE OR REPLACE FUNCTION generate_uuid_v7_precise()
RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
unix_ts_us BIGINT;  -- Микросекунды вместо миллисекунд
    random_a INTEGER;   -- 12-bit random
    random_b BIGINT;    -- 62-bit random
    byte_array BYTEA;
BEGIN
    -- Микросекундная точность для лучшего ordering
    unix_ts_us := EXTRACT(epoch FROM clock_timestamp()) * 1000000;

    -- Используем 44 бита для timestamp (достаточно до 2527 года)
    random_a := (random() * 4095)::INTEGER;  -- 12 бит
    random_b := (random() * 4611686018427387903::BIGINT)::BIGINT;  -- 62 бита

    byte_array :=
        -- Timestamp (44 бита) + Version (4 бита)
        decode(lpad(to_hex((unix_ts_us << 4) | 7), 12, '0'), 'hex') ||
        -- 12 бит random_a + 4 бита padding
        decode(lpad(to_hex(random_a), 4, '0'), 'hex') ||
        -- Variant (2 бита) + 62 бита random_b
        decode(lpad(to_hex((2::BIGINT << 62) | random_b), 16, '0'), 'hex');

RETURN encode(byte_array, 'hex')::UUID;
END;
$$;

-- rollback
DROP FUNCTION IF EXISTS generate_uuid_v7();
DROP FUNCTION IF EXISTS generate_uuid_v7_precise();