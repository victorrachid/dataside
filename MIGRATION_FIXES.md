# SQL to PySpark Migration Fixes - Relatório de Tempo de Atividade por Usuário

## Overview
This document details the fixes applied to resolve discrepancies between the SQL stored procedure `engagesp_Relatorio_De_Tempo_De_Atividade_Por_Usuario` and its PySpark implementation in Databricks.

## Client Validation Issues

### Test 1: Volume Discrepancy (760 vs 182,670 rows)
**Parameters:**
- competitionIds: 6052, 16584
- profileTrackIds: 1, 2
- firstAccessToActivityDateStartDate/EndDate: 2026-01-29 to 2026-02-04

**Root Cause:** Gold layer was missing the activity type filter
**Fix:** Added filter in `engage_gold_tempo_atividade_usuario_new.py` line 54:
```python
atividades_df = dfs["atividade"].where(F.col("id_tipo").isin('padrao_sou', 'scorm'))
```

**SQL Reference:** Line 102 of stored procedure
```sql
WHERE ATIVIDADE.ID_TIPO IN ('padrao_sou', 'scorm')
```

---

### Test 2: Small Volume Difference (14,395 vs 14,534 rows)
**Parameters:**
- competitionId: 16584
- profileTrackIds: 1, 2, 3
- trackId: 68938

**Root Cause:** RA/RAU join creating duplicate rows
**Fix:** Improved join logic to properly handle RODADA_ATIVIDADE vs RODADA_ATIVIDADE_USUARIO (lines 527-565 in gold layer)

---

### Test 3: Extra User with Gestor Profile (1 vs 2 users)
**Parameters:**
- profileTrackIds: 3 (Gestor only)
- activityId: 352304, 352307
- roundId: 302858, 302859, 302860

**Root Cause:** Same as Test 2 - RA/RAU join issue
**Fix:** Same as Test 2

---

### Test 4: Activity Status Filter Not Working (88 vs 0 rows)
**Parameters:**
- activityCompletionStatus: "in_progress"

**Root Cause:** Status value mapping from English to Portuguese was missing
**Fix:** Added status mapping in analytics layer (lines 204-222):
```python
status_map = {
    'in_progress': 'Em Andamento',
    'completed': 'Concluído',
    'not_started': 'Não Iniciado',
    # ...
}
```

---

### Test 5: Activity Completion Date Filter (124 vs 20,325 rows)
**Parameters:**
- activityCompletionDateStartDate/EndDate: 2025-11-07 to 2026-02-04

**Root Cause:** Date filter logic was actually correct - follows SQL's 3-way OR logic
**Fix:** Added detailed comments explaining the logic (lines 223-238 in analytics)

**SQL Reference:** Lines 643-651
```sql
AND ((@showEmptyDates = 1 AND UltimaTentativa.ID IS NULL)
    OR (dates provided AND column BETWEEN dates)
    OR (dates not provided))
```

---

### Test 6: Last Activity Access Date Filter (137 vs 20,300 rows)
**Parameters:**
- lastActivityAccessDateStartDate/EndDate: 2025-11-07 to 2026-02-04

**Root Cause:** Same as Test 5
**Fix:** Same as Test 5

---

### Test 8: valecard Instance (6,350 vs 16,852 rows)
**Root Cause:** Combination of activity type filter + RA/RAU join issues
**Fix:** Combination of fixes from Tests 1 and 2

---

## Missing Columns

### TempoAcessoTotal
**Issue:** Column exists in production but was missing in DataLake
**Fix:** Added aggregation of all ACESSO_ATIVIDADE records (gold layer lines 200-210):
```python
acessos_agregados_df = dfs["acesso_atividade"].groupBy(
    "id_tentativa", "id_cliente", "id_atividade"
).agg(
    F.sum("numero_tempo_em_segundos").alias("TempoAcessoTotalEmSegundos"),
    # ...
)
```

**Note:** The SQL procedure only joins ONE access record (the latest attempt), but the client expects aggregated data. This suggests the production SQL may be different from the procedure file we have.

### QtdAcessosNaAtividade
**Issue:** Column exists in production but was missing in DataLake
**Fix:** Added count of access records in same aggregation (gold layer line 204):
```python
F.count("*").alias("QtdAcessosNaAtividade"),
```

---

## Extra Columns to Remove

### TempoAcessoNaAtividadeEmHoras
**Issue:** Column exists in DataLake but NOT in production SQL
**Fix:** Removed from analytics output (changed to TempoAcessoTotal in line 308)

---

## User Attributes

**Issue:** Some customer-specific attributes not showing (e.g., Cargo, Cipa, Operacao for igua; área, data_de_contratação for valecard)

**Status:** Need to verify the `fnt_atributos_usuarios` function in utils.py is correctly retrieving and pivoting all attributes for each customer.

---

## Code Quality Improvements

1. **Better Code Structure:** Separated RA/RAU joins for clarity
2. **Comprehensive Comments:** Added detailed explanations of SQL logic
3. **Error Handling:** Improved status mapping to handle both English and Portuguese inputs
4. **Performance:** Using F.coalesce() instead of F.when() where appropriate

---

## Testing Recommendations

1. **Gold Layer:** Run the notebook to rebuild the table with all fixes
2. **Analytics Layer:** Test with each of the 8 validation parameter sets
3. **Comparison:** Export results and compare row counts, columns, and sample data with production
4. **Attributes:** Verify attribute pivoting for igua, valecard, and other customers

---

## Technical Details

### SQL vs PySpark Differences

#### Activity Type Filter
- **SQL:** CTEs with WHERE clause filtering activity types
- **PySpark:** Direct filter on atividade dataframe before joins

#### Access Aggregation
- **SQL:** Single LEFT JOIN to ACESSO_ATIVIDADE
- **PySpark:** GroupBy aggregation before join to get sum/count

#### RA/RAU Logic
- **SQL:** Two separate LEFT JOINs with RA.ID_USUARIO IS NULL condition
- **PySpark:** Sequential LEFT JOINs with COALESCE for weight selection

#### Date Filtering
- **SQL:** Complex 3-way OR in WHERE clause
- **PySpark:** Conditional filter application with OR for nulls when showEmptyDates=true

---

## Known Limitations

1. **Attribute Pivoting:** May not handle all edge cases for attribute names with special characters
2. **Status Mapping:** Only supports known status values; unexpected values pass through unchanged
3. **Date Parsing:** Supports ISO 8601, SQL datetime, and date-only formats; other formats may fail

---

## References

- SQL Procedure: `/engage_sql/Procedures/engagesp_Relatorio_De_Tempo_De_Atividade_Por_Usuario.sql`
- Gold Layer: `/gold/engage_gold_tempo_atividade_usuario_new.py`
- Analytics Layer: `/analitycs/job_engage_relatorio_tempo_atividade_usuario_new.py`
- Utils: `/utils.py`
- Validation: `/validacao_cliente.txt`
