# Fix usuario con múltiples cursos asignados

Cambios aplicados:

- Un usuario no administrador ya no queda forzado a un solo `current_user.curso`.
- Si tiene varios permisos en `usuario_roles_curso`, por defecto ve **todos sus cursos asignados**.
- Si usa filtro de `colegio_id` o `curso`, solo puede filtrar dentro de sus permisos reales.
- Las consultas de movimientos, cuotas/morosidad y listados simples aplican aislamiento por pares `colegio + curso`.
- En creación/edición de alumno, usuarios con permisos por curso pueden seleccionar colegio permitido y luego curso permitido.
- Si un mismo curso existe en más de un colegio, el colegio debe venir seleccionado; si no, el backend rechaza la operación para evitar asignación ambigua.
- Se mantiene compatibilidad con usuarios antiguos que solo tienen `usuarios.colegio_id` y `usuarios.curso`.

Recomendación de uso:

1. En Usuarios, asignar permisos por cada combinación colegio + curso.
2. Para usuarios con más de un curso, dejar que usen el filtro superior de curso/colegio.
3. Si no seleccionan curso, verán todos sus cursos asignados.
