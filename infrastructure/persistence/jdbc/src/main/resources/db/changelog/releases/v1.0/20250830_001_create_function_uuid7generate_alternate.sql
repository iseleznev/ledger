-- liquibase formatted sql

-- changeset Igor_Seleznyov:20250825_002_create_function_uuid7generate
-- comment: Create UUID v7 generation functions with fixed timestamp handling

-- Standard UUID v7 function with millisecond precision
CREATE OR REPLACE FUNCTION generate_uuid_v7_alternate()
RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
unix_ts_ms BIGINT;
    rand_a INTEGER;
    rand_b BIGINT;
    uuid_bytes BYTEA;
BEGIN
    -- Get current timestamp in milliseconds since Unix epoch
    unix_ts_ms := EXTRACT(epoch FROM clock_timestamp()) * 1000;

    -- Generate random values
    rand_a := (random() * 4095)::INTEGER;  -- 12 bits
    rand_b := (random() * 1152921504606846975::BIGINT)::BIGINT;  -- 62 bits

    -- Build UUID bytes according to RFC 4122 variant for UUID v7:
    -- 48 bits timestamp_high + timestamp_low
    -- 4 bits version (0111 = 7)
    -- 12 bits random_a
    -- 2 bits variant (10)
    -- 62 bits random_b
    uuid_bytes :=
        -- Timestamp (48 bits) - high 32 bits
        decode(lpad(to_hex((unix_ts_ms >> 16)::INTEGER), 8, '0'), 'hex') ||
        -- Timestamp (48 bits) - low 16 bits + version (4 bits) + rand_a high 4 bits
        decode(lpad(to_hex(((unix_ts_ms & x'FFFF'::BIGINT)::INTEGER << 16) | (7 << 12) | (rand_a >> 8)), 8, '0'), 'hex') ||
        -- rand_a low 8 bits + variant (2 bits) + rand_b high 6 bits
        decode(lpad(to_hex(((rand_a & 255) << 8) | (2 << 6) | ((rand_b >> 56) & 63)), 4, '0'), 'hex') ||
        -- rand_b remaining 56 bits
        decode(lpad(to_hex(rand_b & x'00FFFFFFFFFFFFFF'::BIGINT), 14, '0'), 'hex');

RETURN encode(uuid_bytes, 'hex')::UUID;
END;
$$;

-- High-precision UUID v7 function with microsecond precision and collision avoidance
CREATE OR REPLACE FUNCTION generate_uuid_v7_precise_alternate()
RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
unix_ts_ms BIGINT;
    sub_ms_precision INTEGER;
    rand_a INTEGER;
    rand_b BIGINT;
    uuid_bytes BYTEA;
BEGIN
    -- FIXED: Use milliseconds for timestamp (48 bits max)
    unix_ts_ms := EXTRACT(epoch FROM clock_timestamp()) * 1000;

    -- Add sub-millisecond precision using microseconds within the millisecond
    -- This gives us better ordering within the same millisecond
    sub_ms_precision := (EXTRACT(microseconds FROM clock_timestamp()) % 1000)::INTEGER;

    -- Generate random values
    -- Use sub-millisecond precision in high bits of rand_a for better collision avoidance
    rand_a := ((sub_ms_precision << 2) | (random() * 3)::INTEGER) & 4095;  -- 12 bits total
    rand_b := (random() * 1152921504606846975::BIGINT)::BIGINT;  -- 62 bits

    -- Validate timestamp doesn't overflow 48 bits
    IF unix_ts_ms >= (1::BIGINT << 48) THEN
        RAISE EXCEPTION 'Timestamp overflow: % exceeds 48-bit limit', unix_ts_ms;
END IF;

    -- Build UUID bytes with proper bit allocation
    uuid_bytes :=
        -- Timestamp high 32 bits (bits 127-96)
        decode(lpad(to_hex((unix_ts_ms >> 16)::INTEGER), 8, '0'), 'hex') ||
        -- Timestamp low 16 bits + version 7 (bits 95-64)
        decode(lpad(to_hex(((unix_ts_ms & x'FFFF'::BIGINT)::INTEGER << 16) | (7 << 12) | rand_a), 8, '0'), 'hex') ||
        -- Variant bits (10) + rand_b high 14 bits (bits 63-32)
        decode(lpad(to_hex((2::BIGINT << 30) | ((rand_b >> 32) & x'3FFFFFFF'::BIGINT)), 8, '0'), 'hex') ||
        -- rand_b low 32 bits (bits 31-0)
        decode(lpad(to_hex(rand_b & x'FFFFFFFF'::BIGINT), 8, '0'), 'hex');

RETURN encode(uuid_bytes, 'hex')::UUID;
END;
$$;

-- Utility function to extract timestamp from UUID v7 (for debugging/monitoring)
CREATE OR REPLACE FUNCTION extract_uuid_v7_timestamp_alternate(uuid_val UUID)
RETURNS TIMESTAMP WITH TIME ZONE
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
uuid_bytes BYTEA;
    timestamp_ms BIGINT;
BEGIN
    -- Convert UUID to bytes
    uuid_bytes := decode(replace(uuid_val::TEXT, '-', ''), 'hex');

    -- Extract 48-bit timestamp from first 6 bytes
    timestamp_ms :=
        (get_byte(uuid_bytes, 0)::BIGINT << 40) |
        (get_byte(uuid_bytes, 1)::BIGINT << 32) |
        (get_byte(uuid_bytes, 2)::BIGINT << 24) |
        (get_byte(uuid_bytes, 3)::BIGINT << 16) |
        (get_byte(uuid_bytes, 4)::BIGINT << 8) |
        get_byte(uuid_bytes, 5)::BIGINT;

    -- Convert milliseconds to timestamp
RETURN to_timestamp(timestamp_ms / 1000.0);
END;
$$;

-- Validate UUID v7 format (for testing)
CREATE OR REPLACE FUNCTION validate_uuid_v7_alternate(uuid_val UUID)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
uuid_bytes BYTEA;
    version_byte INTEGER;
    variant_byte INTEGER;
BEGIN
    uuid_bytes := decode(replace(uuid_val::TEXT, '-', ''), 'hex');

    -- Check version bits (should be 0111 = 7)
    version_byte := get_byte(uuid_bytes, 6);
    IF (version_byte >> 4) != 7 THEN
        RETURN FALSE;
END IF;

    -- Check variant bits (should be 10xxxxxx)
    variant_byte := get_byte(uuid_bytes, 8);
    IF (variant_byte >> 6) != 2 THEN
        RETURN FALSE;
END IF;

RETURN TRUE;
END;
$$;

-- Performance test function
CREATE OR REPLACE FUNCTION test_uuid_v7_generation_alternate(count_limit INTEGER DEFAULT 1000)
RETURNS TABLE (
    method TEXT,
    generation_time_ms NUMERIC,
    collision_count INTEGER,
    ordering_correct BOOLEAN
)
LANGUAGE plpgsql
AS $$
DECLARE
start_time TIMESTAMP;
    end_time TIMESTAMP;
    uuid_array UUID[];
    i INTEGER;
    collisions INTEGER := 0;
    ordered BOOLEAN := TRUE;
BEGIN
    -- Test standard UUID v7
    uuid_array := ARRAY[]::UUID[];
    start_time := clock_timestamp();

FOR i IN 1..count_limit LOOP
        uuid_array := array_append(uuid_array, generate_uuid_v7());
END LOOP;

    end_time := clock_timestamp();

    -- Check for collisions
SELECT count_limit - count(DISTINCT unnest) INTO collisions
FROM unnest(uuid_array);

-- Check ordering (UUIDs should be in ascending order when generated sequentially)
FOR i IN 2..count_limit LOOP
        IF uuid_array[i] < uuid_array[i-1] THEN
            ordered := FALSE;
            EXIT;
END IF;
END LOOP;

    method := 'generate_uuid_v7';
    generation_time_ms := EXTRACT(epoch FROM (end_time - start_time)) * 1000;
    collision_count := collisions;
    ordering_correct := ordered;
    RETURN NEXT;

    -- Test precise UUID v7
    uuid_array := ARRAY[]::UUID[];
    start_time := clock_timestamp();
    ordered := TRUE;

FOR i IN 1..count_limit LOOP
        uuid_array := array_append(uuid_array, generate_uuid_v7_precise());
END LOOP;

    end_time := clock_timestamp();

SELECT count_limit - count(DISTINCT unnest) INTO collisions
FROM unnest(uuid_array);

FOR i IN 2..count_limit LOOP
        IF uuid_array[i] < uuid_array[i-1] THEN
            ordered := FALSE;
            EXIT;
END IF;
END LOOP;

    method := 'generate_uuid_v7_precise';
    generation_time_ms := EXTRACT(epoch FROM (end_time - start_time)) * 1000;
    collision_count := collisions;
    ordering_correct := ordered;
    RETURN NEXT;
END;
$$;

-- Add helpful comments
COMMENT ON FUNCTION generate_uuid_v7() IS 'Generate UUID v7 with millisecond timestamp precision, RFC 4122 compliant';
COMMENT ON FUNCTION generate_uuid_v7_precise() IS 'Generate UUID v7 with sub-millisecond precision for collision avoidance';
COMMENT ON FUNCTION extract_uuid_v7_timestamp(UUID) IS 'Extract timestamp from UUID v7 for debugging';
COMMENT ON FUNCTION validate_uuid_v7(UUID) IS 'Validate UUID v7 format compliance';
COMMENT ON FUNCTION test_uuid_v7_generation(INTEGER) IS 'Performance test UUID v7 generation functions';

-- rollback
DROP FUNCTION IF EXISTS generate_uuid_v7_alternate();
DROP FUNCTION IF EXISTS generate_uuid_v7_precise_alternate();
DROP FUNCTION IF EXISTS extract_uuid_v7_timestamp_alternate(UUID);
DROP FUNCTION IF EXISTS validate_uuid_v7_alternate(UUID);
DROP FUNCTION IF EXISTS test_uuid_v7_generation_alternate(INTEGER);