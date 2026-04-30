# Cambios multi-colegio

Esta versión agrega soporte para más de un colegio sin borrar la data actual.

## Qué hace la migración al iniciar

1. Crea la tabla `colegios`.
2. Crea el colegio inicial usando:
   - `Escuela Las Mercedes`
   - `María Pinto`
3. Agrega `colegio_id` a usuarios, alumnos, actividades y movimientos.
4. Asigna todos los datos existentes al primer colegio.
5. Cambia el índice único de alumnos para permitir el mismo alumno/curso en distintos colegios.

## Uso

- Entra como administrador.
- Ve a **Colegios** para crear o editar colegios.
- En el selector superior puedes ver todos los colegios o filtrar por uno.
- Al crear usuarios, asígnales un colegio.
- Los usuarios que no son admin quedan limitados a su colegio y curso.

## Recomendación antes de desplegar

Haz un respaldo de la base actual antes de iniciar esta versión por primera vez.
La migración está diseñada para conservar datos, pero el respaldo permite volver atrás si el hosting interrumpe el despliegue.
