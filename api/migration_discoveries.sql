-- ============================================================
-- MIGRATION : Peupler cm_discoveries depuis creatures_v2
-- À exécuter UNE SEULE FOIS après le déploiement du nouveau main.py
-- ============================================================

-- 1. Crée les tables si elles n'existent pas encore
--    (normalement fait au démarrage de l'API, mais par sécurité)
CREATE TABLE IF NOT EXISTS cm_discoveries (
    twitch_login  TEXT NOT NULL,
    cm_key        TEXT NOT NULL REFERENCES cms(key) ON DELETE CASCADE,
    stage         INT  NOT NULL,
    status        TEXT NOT NULL DEFAULT 'owned'
                  CHECK (status IN ('owned', 'discovered')),
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (twitch_login, cm_key, stage)
);

CREATE TABLE IF NOT EXISTS viewer_presence (
    twitch_login TEXT NOT NULL,
    week_start   DATE NOT NULL,
    seconds      INT  NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (twitch_login, week_start)
);

-- ============================================================
-- 2. Insérer les formes ACTUELLEMENT POSSÉDÉES (status = 'owned')
--    Pour chaque viewer, son CM actif au stage actuel
-- ============================================================
INSERT INTO cm_discoveries (twitch_login, cm_key, stage, status, discovered_at)
SELECT
    c.twitch_login,
    c.cm_key,
    f.stage,
    'owned',
    COALESCE(c.updated_at, now())
FROM creatures_v2 c
JOIN cm_forms f ON f.cm_key = c.cm_key AND f.stage = c.stage
WHERE c.cm_key != 'egg'
  AND c.stage >= 1
ON CONFLICT (twitch_login, cm_key, stage) DO NOTHING;

-- ============================================================
-- 3. Insérer les SOUS-ÉVOLUTIONS (status = 'discovered')
--    Ex : si quelqu'un a un Aigrivière (stage 2), on marque
--    son Aigruisseau (stage 1) comme 'discovered'
-- ============================================================
INSERT INTO cm_discoveries (twitch_login, cm_key, stage, status, discovered_at)
SELECT
    c.twitch_login,
    c.cm_key,
    f.stage,
    'discovered',
    COALESCE(c.updated_at, now())
FROM creatures_v2 c
JOIN cm_forms f ON f.cm_key = c.cm_key
WHERE c.cm_key != 'egg'
  AND c.stage >= 1
  AND f.stage < c.stage   -- toutes les formes inférieures au stage actuel
  AND f.stage >= 1
ON CONFLICT (twitch_login, cm_key, stage) DO UPDATE
    SET status = CASE
        WHEN cm_discoveries.status = 'owned' THEN 'owned'  -- ne pas rétrograder
        ELSE 'discovered'
    END;

-- ============================================================
-- Vérification : nombre de découvertes insérées
-- ============================================================
SELECT
    status,
    COUNT(*) AS nb_formes,
    COUNT(DISTINCT twitch_login) AS nb_viewers
FROM cm_discoveries
GROUP BY status
ORDER BY status;

-- ============================================================
-- Détail par viewer (optionnel, pour vérifier)
-- ============================================================
-- SELECT twitch_login, cm_key, stage, status
-- FROM cm_discoveries
-- ORDER BY twitch_login, cm_key, stage
-- LIMIT 50;
