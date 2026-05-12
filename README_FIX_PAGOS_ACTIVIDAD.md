# Fix pagos asociados a actividades

Se corrigió un error 500 al registrar desde la pestaña **Pagos** un aporte asociado a **alumno + actividad**.

## Causa
La validación `actividad_permitida_para_alumno` usaba una función inexistente (`normalizar_curso_texto`) para comparar los cursos del alumno y la actividad. Al ejecutarse esa validación, Flask caía en error interno y mostraba el mensaje genérico: “Revisa los filtros de búsqueda”.

## Corrección
Se reemplazó esa llamada por la función existente `normalize_course`, manteniendo la validación de que alumno y actividad pertenezcan al mismo colegio y curso.

## Resultado
Ahora el registro de aportes a actividades desde **Pagos > Registrar pago** funciona igual que desde **Actividades > Nuevo ingreso**.
