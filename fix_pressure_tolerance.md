# Fixes para tolerar fallas bajo estrés

A continuación se detallarán tres bugs de recuperación que aparecieron al probar caídas agresivas
(2 nodos cada 3s). Los tres son
la misma clase de problema: el estado reconstruido desde el WAL quedaba
incompleto o corrupto. No aparecían antes porque dependen de caer en una ventana
muy chica y/o de recuperar el mismo nodo más de una vez, algo que sólo se da con
caídas muy frecuentes. Hay que tener en cuenta que éstas situaciones sólo suceden si las caídas consecutivas afectan al mismo cliente. 

## 1. Registro corrupto al final del WAL

Cada append escribe una línea JSON terminada en `\n` con `flush` + `fsync`. Una
escritura no es atómica: si el proceso muere justo en el medio, queda un registro
a medias (sin `\n`) al final del archivo. Al recuperar, `json.loads` rompía
sobre esa línea y el worker no volvía a revivir a pesar de los intentos del `Watchdog`.

**Fix:** `load()` lee registro por registro; si la última línea no termina en
`\n` la descarta y trunca el archivo en disco (con `fsync`) hasta el último
registro completo. Como el WAL es append-only, el único registro que puede estar
roto es el último.

## 2. Doble conteo en el RingEOF

El anillo de terminación acumula *deltas*: cada nodo aporta sólo lo nuevo desde
la última vuelta (`count - partial`) y avanza su marca `partial`. Esa marca se
persistía al recibir el token por el canal de control, pero **no** al iniciar el
anillo desde el canal main. Si un nodo iniciaba el anillo y se caía antes de
recibir el token de vuelta, al recuperar perdía su `partial`, por lo tanto, volvía a aportar
su delta, provocando un conteo inflado y que la query nunca termine.

**Fix:** al iniciar el anillo se persiste el snapshot de
control con los datos de conteo ya avanzados, al igual que se hace en el camino de control.

## 3. Side input perdido tras compactar

La tabla de lookup del side input y sus contadores (`received` / `expected`) sólo
vivían en los registros por mensaje del WAL. La compactación (y `_recover`, que
compacta al final) los reemplazaba por un snapshot que **conservaba el `seen`
pero descartaba la tabla y los contadores**. Tras una segunda recuperación, los
mensajes quedaban marcados como vistos (no se reprocesaban) pero la tabla estaba
vacía, por lo tanto el side input nunca llegaba a "ready" y el worker se colgaba. 

**Fix:** el snapshot ahora incluye `side_received`, `side_expected` y la tabla
entera (`_side_state_as_delta`, reusando el mismo formato de delta). Al restaurar,
se rearma la tabla con `_apply_side_delta` y se llama a `SideInputTracker.restore`,
que reconstruye los contadores y vuelve a disparar el evento "ready" si el side
input ya estaba completo.
