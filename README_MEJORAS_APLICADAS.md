# Mejoras aplicadas - ContaCurso

Esta versión agrega mejoras de administración y trazabilidad:

## 1. Auditoría de acciones
Se creó la tabla `auditoria_acciones` y una nueva vista para el usuario `admin`:

- Menú: **Auditoría**
- Registra inicio/cierre de sesión
- Registra creación/edición de colegios
- Registra creación de alumnos
- Registra creación de pagos
- Registra respaldos manuales

La auditoría guarda fecha, usuario, acción, entidad, colegio, curso, detalle e IP.

## 2. Respaldos
Se mantiene el panel de respaldos y ahora cada respaldo manual queda registrado en auditoría.

## 3. Compatibilidad
La migración crea automáticamente la tabla de auditoría si no existe. No elimina datos existentes.

## Recomendación
Antes de reemplazar tu versión en producción, respalda la base actual:

```bash
cp instance/contabilidad_curso.db instance/contabilidad_curso_backup.db
```
