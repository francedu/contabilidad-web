# Cursos con letras y eliminación segura

Cambios aplicados:

- Los cursos ahora se crean con:
  - Curso base: Prekínder, Kínder, 1° Básico ... 4° Medio.
  - Letra: Sin letra, A, B, C, D, E, F.
- El nombre final se compone automáticamente:
  - 1° Medio + A = 1° Medio A
  - 1° Medio + Sin letra = 1° Medio
- Se mantiene compatibilidad con cursos antiguos guardados como texto.
- Se agregan columnas `curso_base` y `letra` a la tabla `cursos` con migración segura.
- Los cursos existentes se completan automáticamente leyendo el nombre actual.
- Se agrega botón **Eliminar** en Cursos.
- La eliminación es segura:
  - Si el curso tiene alumnos, movimientos, actividades, cuotas, cierres o permisos asociados, no se elimina.
  - En ese caso se recomienda usar **Desactivar**.

Uso recomendado:

1. Crear colegio.
2. El sistema crea cursos base sin letra.
3. Si necesitas paralelos, crea por ejemplo:
   - 1° Básico A
   - 1° Básico B
4. Usa "Sin letra" cuando el colegio no maneje paralelos.

