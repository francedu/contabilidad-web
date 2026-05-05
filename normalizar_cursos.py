#!/usr/bin/env python3
"""
Normalizador seguro de cursos para ContaCurso.

Uso recomendado en Render Shell o local con DATABASE_URL:

  python normalizar_cursos.py              # simulación, no modifica nada
  python normalizar_cursos.py --apply      # aplica cambios
  python normalizar_cursos.py --apply --seccion A

Qué hace:
- Crea cursos base por colegio: Prekínder, Kínder, 1° Básico a 8° Básico, 1° Medio a 4° Medio.
- Conserva datos existentes.
- Normaliza textos antiguos de curso en alumnos, pagos/movimientos, actividades, permisos, cierres y cuotas.
- No borra cursos ni datos.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import unicodedata
from datetime import datetime
from typing import Iterable

try:
    import psycopg2
    import psycopg2.extras
except Exception as exc:  # pragma: no cover
    print("ERROR: falta psycopg2-binary. Ejecuta: pip install -r requirements.txt", file=sys.stderr)
    raise

BASE_COURSES = [
    "Prekínder", "Kínder",
    "1° Básico", "2° Básico", "3° Básico", "4° Básico",
    "5° Básico", "6° Básico", "7° Básico", "8° Básico",
    "1° Medio", "2° Medio", "3° Medio", "4° Medio",
]

# Tablas que guardan curso como texto. No se elimina nada; solo se actualiza el texto.
TABLES_WITH_COURSE = [
    "alumnos",
    "movimientos",
    "actividades",
    "usuario_roles_curso",
    "cierres_mensuales",
    "cuotas_mensuales",
]


def strip_accents(value: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFD", value) if unicodedata.category(ch) != "Mn")


def key(value: str | None) -> str:
    value = (value or "").replace("\u00a0", " ").strip().lower()
    value = strip_accents(value)
    value = value.replace("º", "°")
    value = re.sub(r"[^a-z0-9°]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def canonical_course(raw: str | None, seccion: str = "") -> str:
    """Devuelve nombre canónico. Si no reconoce el curso, conserva el texto limpio."""
    original = (raw or "").replace("\u00a0", " ").strip()
    k = key(original)
    if not k:
        return ""

    if k in {"prekinder", "pre kinder", "pre kinder", "prekindergarten", "pre escolar"}:
        base = "Prekínder"
    elif k in {"kinder", "kindergarten"}:
        base = "Kínder"
    else:
        base = ""
        # Básica: 1 basico, 1° basico, primero basico, etc.
        palabras_basica = {
            1: ("1", "1°", "1ro", "primero"),
            2: ("2", "2°", "2do", "segundo"),
            3: ("3", "3°", "3ro", "tercero"),
            4: ("4", "4°", "4to", "cuarto"),
            5: ("5", "5°", "5to", "quinto"),
            6: ("6", "6°", "6to", "sexto"),
            7: ("7", "7°", "7mo", "septimo", "séptimo"),
            8: ("8", "8°", "8vo", "octavo"),
        }
        palabras_medio = {
            1: ("1 medio", "1° medio", "primero medio", "i medio"),
            2: ("2 medio", "2° medio", "segundo medio", "ii medio"),
            3: ("3 medio", "3° medio", "tercero medio", "iii medio"),
            4: ("4 medio", "4° medio", "cuarto medio", "iv medio"),
        }
        for n, variants in palabras_basica.items():
            if any(k == key(v) or k.startswith(key(v) + " ") for v in variants) and "medio" not in k:
                if "basico" in k or "basica" in k or k.split()[0] in {key(v) for v in variants}:
                    base = f"{n}° Básico"
                    break
        if not base:
            for n, variants in palabras_medio.items():
                if any(k == key(v) or k.startswith(key(v) + " ") for v in variants):
                    base = f"{n}° Medio"
                    break

    if not base:
        # Conserva cursos personalizados, pero limpia espacios.
        return re.sub(r"\s+", " ", original)

    seccion = (seccion or "").strip().upper()
    if seccion and base not in {"Prekínder", "Kínder"} and not re.search(r"\b[A-Z]$", base):
        return f"{base} {seccion}"
    return base


def connect():
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: no existe DATABASE_URL. Ejecuta esto en Render Shell o define DATABASE_URL localmente.", file=sys.stderr)
        sys.exit(2)
    return psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)


def ensure_cursos_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cursos (
            id BIGSERIAL PRIMARY KEY,
            colegio_id BIGINT NOT NULL,
            nombre TEXT NOT NULL,
            nivel TEXT,
            orden INTEGER DEFAULT 999,
            activo INTEGER DEFAULT 1,
            UNIQUE(colegio_id, nombre)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cursos_colegio_nombre ON cursos(colegio_id, nombre)")


def colegios(cur, solo_colegio: int | None = None):
    if solo_colegio:
        cur.execute("SELECT id, nombre FROM colegios WHERE id = %s ORDER BY nombre", (solo_colegio,))
    else:
        cur.execute("SELECT id, nombre FROM colegios ORDER BY nombre")
    return cur.fetchall()


def table_has_column(cur, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
        """,
        (table, column),
    )
    return cur.fetchone() is not None


def distinct_courses(cur, colegio_id: int) -> set[str]:
    valores: set[str] = set()
    for table in TABLES_WITH_COURSE:
        if not table_has_column(cur, table, "curso"):
            continue
        if table_has_column(cur, table, "colegio_id"):
            cur.execute(f"SELECT DISTINCT curso FROM {table} WHERE colegio_id = %s AND curso IS NOT NULL AND trim(curso) <> ''", (colegio_id,))
        else:
            cur.execute(f"SELECT DISTINCT curso FROM {table} WHERE curso IS NOT NULL AND trim(curso) <> ''")
        for row in cur.fetchall():
            if row["curso"]:
                valores.add(str(row["curso"]).strip())
    return valores


def insert_course(cur, colegio_id: int, nombre: str, nivel: str, orden: int, dry_run: bool):
    if dry_run:
        print(f"  + crearía curso: {nombre}")
        return
    cur.execute(
        """
        INSERT INTO cursos (colegio_id, nombre, nivel, orden, activo)
        VALUES (%s, %s, %s, %s, 1)
        ON CONFLICT (colegio_id, nombre) DO NOTHING
        """,
        (colegio_id, nombre, nivel, orden),
    )


def update_course_refs(cur, colegio_id: int, old: str, new: str, dry_run: bool):
    if old == new:
        return 0
    total = 0
    for table in TABLES_WITH_COURSE:
        if not table_has_column(cur, table, "curso"):
            continue
        if table_has_column(cur, table, "colegio_id"):
            cur.execute(f"SELECT COUNT(*) AS n FROM {table} WHERE colegio_id = %s AND curso = %s", (colegio_id, old))
            n = int(cur.fetchone()["n"] or 0)
            if n and not dry_run:
                cur.execute(f"UPDATE {table} SET curso = %s WHERE colegio_id = %s AND curso = %s", (new, colegio_id, old))
        else:
            cur.execute(f"SELECT COUNT(*) AS n FROM {table} WHERE curso = %s", (old,))
            n = int(cur.fetchone()["n"] or 0)
            if n and not dry_run:
                cur.execute(f"UPDATE {table} SET curso = %s WHERE curso = %s", (new, old))
        total += n
    if total:
        print(f"  ~ {old!r} -> {new!r}: {total} referencias")
    return total


def main():
    parser = argparse.ArgumentParser(description="Normaliza cursos por colegio sin borrar datos.")
    parser.add_argument("--apply", action="store_true", help="Aplica cambios. Sin esto solo muestra simulación.")
    parser.add_argument("--seccion", default="", help="Opcional: agrega sección/letra a Básica/Media, por ejemplo A.")
    parser.add_argument("--colegio-id", type=int, default=None, help="Opcional: procesa solo un colegio.")
    args = parser.parse_args()

    dry_run = not args.apply
    print("ContaCurso - normalización de cursos")
    print("Modo:", "APLICAR CAMBIOS" if args.apply else "SIMULACIÓN / DRY RUN")
    if args.seccion:
        print("Sección a agregar:", args.seccion.upper())
    print()

    conn = connect()
    try:
        with conn:
            with conn.cursor() as cur:
                ensure_cursos_table(cur)
                colegios_rows = colegios(cur, args.colegio_id)
                if not colegios_rows:
                    print("No se encontraron colegios.")
                    return
                for colegio in colegios_rows:
                    cid = int(colegio["id"])
                    print(f"Colegio {cid}: {colegio['nombre']}")

                    # 1) Crear cursos base.
                    for i, base in enumerate(BASE_COURSES, start=1):
                        nombre = canonical_course(base, args.seccion)
                        nivel = "base"
                        insert_course(cur, cid, nombre, nivel, i, dry_run)

                    # 2) Crear/migrar cursos históricos.
                    encontrados = sorted(distinct_courses(cur, cid), key=key)
                    if encontrados:
                        print("  Cursos históricos detectados:", ", ".join(encontrados))
                    for old in encontrados:
                        new = canonical_course(old, args.seccion)
                        if new:
                            insert_course(cur, cid, new, "historico", 999, dry_run)
                            update_course_refs(cur, cid, old, new, dry_run)
                    print()
        if dry_run:
            conn.rollback()
            print("Simulación terminada. No se modificó la base.")
            print("Para aplicar: python normalizar_cursos.py --apply")
        else:
            print("Cambios aplicados correctamente.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
