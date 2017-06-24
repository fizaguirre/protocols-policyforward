#Policy Forward module

## Trabalho para a cadeira de Protocolo de comunicações 2017/1.

Instruções para configurar o projeto.

Clonar o repositorio padrão do floodlight e inicializa-lo corretamente.
```
git clone https://github.com/floodlight/floodlight.git
cd floodlight
git submodule init
git submodule update
```

Clonar o repostitório do modulo do trabalho em:
floodlight/src/main/java/net/floodlightcontroller/
Executando:
```
git clone https://github.com/fizaguirre/protocols-policyforward.git policyforward
```

Importar o projeto do floodlight no eclipse. Na arvore de pacotes do em /src/main/java deve haver um pacote policyforward.

Adicionar no arquivo /src/main/resources/floodlightdefault.properties o modulo policyforward como exemplo abaixo.
floodlight.modules=\
> ...
> net.floodlightcontroller.statistics.StatisticsCollector,\
> net.floodlightcontroller.policyforward.PolicyForward

Adicionar ao fim do arquivo /src/main/resources/META-INF/services/net.floodlightcontroller.core.module.IFloodlightModule a seguinte linha.

> net.floodlightcontroller.policyforward.PolicyForward

Os arquivos floodlightdefault.properties e net.floodlightcontroller.core.module.IFloodlightModule são responsaveis por comunicar que modulos serão carregados na inicialização do Floodlight. Não precismos de alguns destes modulos, e estes podem interfirar no comportamento do que estamos trabalhando. Portanto desabilitar os modulos ACL, Firewall, e Forwarding removendo a entrada destes modulos dos arquivos floodlightdefault.properties e net.floodlightcontroller.core.module.IFloodlightModule.





