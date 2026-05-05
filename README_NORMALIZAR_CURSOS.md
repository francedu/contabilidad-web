# Normalización segura de cursos por colegio

Se agregó el script `normalizar_cursos.py` para ordenar cursos sin romper la data existente.

## Qué hace

- Crea cursos base por cada colegio:
  - Prekínder
  - Kínder
  - 1° Básico a 8° Básico
  - 1° Medio a 4° Medio
- Detecta cursos antiguos usados en:
  - alumnos
  - movimientos
  - actividades
  - usuario_roles_curso
  - cuotas_mensuales
  - cierres_mensuales
- Normaliza nombres duplicados o escritos distinto.
- No borra datos.
- No elimina cursos.

## Uso seguro en Render

Primero abre **Shell** en Render y ejecuta simulación:

```bash
python normalizar_cursos.py
```

Eso solo muestra lo que haría.

Para aplicar cambios:

```bash
python normalizar_cursos.py --apply
```

## Agregar letra/sección A

Si quieres que los cursos queden como `1° Básico A`, `2° Básico A`, `1° Medio A`, etc.:

```bash
python normalizar_cursos.py --apply --seccion A
```

Recomendación: usa `--seccion A` solo si ese colegio tiene un solo paralelo por nivel y quieres dejarlo preparado para paralelos futuros.

## Procesar un solo colegio

```bash
python normalizar_cursos.py --apply --colegio-id 1
```

## Importante

Antes de ejecutar con `--apply`, haz backup de PostgreSQL desde Render.
