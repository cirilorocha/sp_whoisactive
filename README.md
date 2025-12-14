# sp_WhoIsActive
[![licence badge]][licence]
[![stars badge]][stars]
[![forks badge]][forks]
[![issues badge]][issues]

`sp_WhoIsActive` is a comprehensive activity monitoring stored procedure that works for all versions of SQL Server from 2005 through 2022 and Azure SQL DB.

`sp_WhoIsActive` is now officially versioned and maintained here on GitHub.

The license is now [GPLv3](/LICENSE).

Documentation is still available at http://whoisactive.com/docs

If you have enhancements, please consider a PR instead of a fork. I would like to continue to maintain this project for the community.

[licence badge]:https://img.shields.io/badge/license-GPLv3-blue.svg
[stars badge]:https://img.shields.io/github/stars/amachanic/sp_whoisactive.svg
[forks badge]:https://img.shields.io/github/forks/amachanic/sp_whoisactive.svg
[issues badge]:https://img.shields.io/github/issues/amachanic/sp_whoisactive.svg

[licence]:https://github.com/amachanic/sp_whoisactive/blob/master/LICENSE
[stars]:https://github.com/amachanic/sp_whoisactive/stargazers
[forks]:https://github.com/amachanic/sp_whoisactive/network
[issues]:https://github.com/amachanic/sp_whoisactive/issues

O script é uma _stored procedure_ amplamente utilizada para monitorização no SQL Server. A documentação está dividida em três partes principais: **Cabeçalho**, **Parâmetros** (como configurar a execução) e **Colunas de Saída** (o que os resultados significam).


### 1. Comentários dos Parâmetros (Configuração)

Estes comentários explicam o que cada opção faz ao executar a _procedure_.

* **@filter**:
  
  * Filtros — Tanto inclusivos como exclusivos.
  
  * Defina qualquer filtro como `''` para desativar.
  
  * Os tipos de filtro válidos são: `session` (sessão), `program` (programa), `database` (base de dados), `login` e `host`.
  
  * `Session` é um ID de sessão, e `0` ou `''` podem ser usados para indicar "todas" as sessões.
  
  * Todos os outros tipos de filtro suportam `%` ou `_` como _wildcards_ (caracteres curinga).

* **@show_own_spid**: Recuperar dados sobre a sessão que está a fazer a chamada?

* **@show_system_spids**: Recuperar dados sobre sessões de sistema?

* **@show_sleeping_spids**:
  
  * Controla como os SPIDs adormecidos (_sleeping_) são tratados, com base na ideia de níveis de interesse.
  
  * `0`: não recolhe quaisquer SPIDs adormecidos.
  
  * `1`: recolhe apenas os SPIDs adormecidos que também tenham uma transação aberta.
  
  * `2`: recolhe todos os SPIDs adormecidos.

* **@get_full_inner_text**:
  
  * Se `1`, obtém o procedimento armazenado completo ou o lote (_batch_) em execução, quando disponível.
  
  * Se `0`, obtém apenas a instrução real que está a ser executada no momento dentro do lote ou procedimento.

* **@get_plans**:
  
  * Obter planos de consulta associados para tarefas em execução, se disponíveis.
  
  * Se `@get_plans = 1`, obtém o plano com base no _offset_ (deslocamento) da instrução do pedido.
  
  * Se `@get_plans = 2`, obtém o plano inteiro com base no `plan_handle` do pedido.

* **@get_outer_command**: Obter a consulta _ad hoc_ externa ou a chamada de procedimento armazenado associada, se disponível.

* **@get_transaction_info**: Ativa a extração de informações de escrita no registo de transações (_log_), duração da transação e a coluna de identificação `implicit_transaction`.

* **@get_task_info**:
  
  * Obter informações sobre tarefas ativas, com base em três níveis de interesse.
  
  * Nível `0`: não recolhe qualquer informação relacionada com tarefas.
  
  * Nível `1`: é um modo leve que recolhe a principal espera (_wait_) que não seja CXPACKET, dando preferência a bloqueadores.
  
  * Nível `2`: recolhe todas as métricas baseadas em tarefas disponíveis, incluindo: número de tarefas ativas, estatísticas de espera atuais, E/S física (_physical I/O_), mudanças de contexto (_context switches_) e informações sobre bloqueadores.

* **@get_locks**: Obtém bloqueios associados para cada pedido, agregados num formato XML.

* **@get_avg_time**:
  
  * Obter tempo médio para execuções passadas de uma consulta ativa.
  
  * (baseado na combinação de `plan handle`, `sql handle` e `offset`).

* **@get_additional_info**:
  
  * Obter informações adicionais não relacionadas com o desempenho sobre a sessão ou pedido (ex: `text_size`, `language`, `date_format`, etc.).
  
  * Se um trabalho (_job_) do SQL Agent estiver em execução, um sub-nó chamado `agent_info` será preenchido com: `job_id`, `job_name`, `step_id`, etc.
  
  * Se `@get_task_info` for `2` e uma espera de bloqueio for detetada, um sub-nó chamado `block_info` será preenchido com detalhes sobre o objeto bloqueado.

* **@get_memory_info**:
  
  * Obter informações adicionais relacionadas com a memória de trabalho (`requested_memory`, `granted_memory`, etc.).
  
  * Não disponível para SQL Server 2005.

* **@find_block_leaders**:
  
  * Percorrer a cadeia de bloqueio e contar o número total de SPIDs bloqueados "fundo abaixo" por uma determinada sessão.
  
  * Também ativa o `task_info` Nível 1, se `@get_task_info` estiver definido como `0`.

* **@delta_interval**:
  
  * Extrair deltas (diferenças) em várias métricas.
  
  * Intervalo em segundos para esperar antes de fazer a segunda recolha de dados.

* **@output_column_list**:
  
  * Lista de colunas de saída desejadas, na ordem desejada.
  
  * A saída final será a interseção de todas as funcionalidades ativadas e colunas listadas.
  
  * Os nomes devem ser delimitados por parênteses retos `[]`.

* **@sort_order**: Coluna(s) pelas quais ordenar a saída, opcionalmente com direções de ordenação.

* **@format_output**:
  
  * Formata algumas das colunas de saída numa forma mais "legível para humanos".
  
  * `0`: desativa a formatação.
  
  * `1`: formata para fontes de largura variável.
  
  * `2`: formata para fontes de largura fixa.

* **@destination_table**: Se definido, o script tentará inserir numa tabela de destino (não verifica se a tabela existe ou tem o esquema correto).

* **@return_schema**: Se `1`, não recolhe dados, mas devolve uma instrução `CREATE TABLE` correspondente ao esquema do resultado.

* **@help**: Ajuda! O que é que eu faço?

* * *

### 2. Comentários das Colunas de Saída

Estes comentários explicam o que cada coluna retornada representa.

* **[session_id]**: ID da Sessão (também conhecido como SPID).

* **[dd hh:mm:ss.mss]**:
  
  * Para um pedido ativo: tempo que a consulta está a ser executada.
  
  * Para uma sessão adormecida: tempo desde que o último lote (_batch_) foi concluído.

* **[dd hh:mm:ss.mss (avg)]**: Quanto tempo a parte ativa da consulta demorou no passado, em média (requer `@get_avg_time`).

* **[physical_io]**: Mostra o número de E/S físicas, para pedidos ativos.

* **[reads]**:
  
  * Pedido ativo: número de leituras feitas para a consulta atual.
  
  * Sessão adormecida: número total de leituras durante a vida da sessão.

* **[writes]**: Semelhante a leituras, mas para escritas.

* **[tempdb_allocations]**: Número de escritas na TempDB.

* **[tempdb_current]**: Número de páginas da TempDB atualmente alocadas.

* **[CPU]**: Tempo total de CPU consumido.

* **[context_switches]**: Número de mudanças de contexto para pedidos ativos.

* **[used_memory]**: Consumo total de memória (para a consulta atual ou total da sessão se adormecida).

* **[requested_memory]**: Memória solicitada pelo processador de consultas (hash, sort, paralelismo).

* **[granted_memory]**: Memória concedida ao processador de consultas.

* **[..._delta]**: (Várias colunas) Diferença na métrica entre a primeira e a segunda recolha (requer `@delta_interval`).

* **[tasks]**: Número de tarefas de trabalho (_worker tasks_) atualmente alocadas.

* **[status]**: Estado da atividade (running, sleeping, etc).

* **[wait_info]**:
  
  * Agrega informações de espera no formato `(Ax: Bms/Cms/Dms)E`.
  
  * `A`: número de tarefas à espera no recurso `E`.
  
  * `B/C/D`: tempos de espera em milissegundos (mínimo/média/máximo).
  
  * Se o tipo de espera for `CXPACKET`, o `nodeId` do plano de consulta é identificado.

* **[locks]**: Informação de bloqueios em formato XML.

* **[tran_start_time]**: Data e hora em que a primeira transação aberta causou uma escrita no log.

* **[tran_log_writes]**: Agrega informações de escrita no log (Base de dados, número de escritas, tamanho em kB).

* **[implicit_tran]**: Se a transação foi iniciada devido à opção `implicit_transactions`.

* **[open_tran_count]**: Número de transações abertas que a sessão tem.

* **[sql_command]**: O comando SQL "externo" (texto do lote ou RPC), se disponível.

* **[sql_text]**:
  
  * Texto SQL para pedidos ativos ou última instrução executada.
  
  * Pode mostrar `<timeout_exceeded />` se o texto estiver bloqueado.

* **[query_plan]**: Plano de consulta (XML).

* **[blocking_session_id]**: O SPID que está a bloquear esta sessão, se aplicável.

* **[blocked_session_count]**: O número total de SPIDs bloqueados por esta sessão (em toda a cadeia).

* **[percent_complete]**: Percentagem de conclusão (ex: para backups, restauros).

* **[host_name]**: Nome do anfitrião (_host_) da ligação.

* **[login_name]**: Nome de _login_ da ligação.

* **[database_name]**: Base de dados ligada.

* **[program_name]**: Nome do programa/aplicação reportado.

* **[start_time]**: Hora de início do pedido (ou fim do último lote para sessões adormecidas).

* **[login_time]**: Hora em que a sessão se ligou.

