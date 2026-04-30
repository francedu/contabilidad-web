# Corrección: datos no visibles después de multi-colegio

Esta versión corrige una migración que podía dejar información antigua fuera de los filtros por colegio.

Cambios aplicados:
- Si `colegio_id` estaba `NULL` o `0`, se reasigna al colegio inicial `1`.
- Los movimientos antiguos se vuelven a enlazar con el colegio del alumno o actividad asociada.
- La lista/exportación de movimientos ahora respeta el filtro de colegio del admin.
- El usuario global sigue siendo `admin`.

Después de reemplazar la app:
1. Haz respaldo de tu base actual.
2. Ejecuta `python app.py` una vez para que corra la migración.
3. Entra como `admin`.
4. En el filtro de colegio, prueba primero “Todos los colegios” y luego el colegio original.

Si aún no ves datos, revisa la base con:

```sql
SELECT colegio_id, COUNT(*), SUM(CASE WHEN tipo='ingreso' THEN monto ELSE 0 END) ingresos
FROM movimientos
GROUP BY colegio_id;
```
