## Galileu Agents
Galileu Agents é um conjunto de módulos responsáveis pela captura de informações relacionadas ao consumo de recursos, assessment dos equipamentos, análise de eventos e obtenção do health status de um rede WiFi, incialmente utilizando APIs Meraki (Cisco). Basicamente, existem 4 programas de coleta, um dispatcher para controle de execução destes coletores, além de um arquivo de configuração. 
Os dados, resultantes destas coletas, são enviados única e exclusivamente, utilizando um conjunto de APIs, ao servidor backend.  

### 1. Requirements
Garanta que a versão do python seja igual ou superior à 3.9
Todos os arquivos devem ser instalados sob o diretório "/usr/local/WOC"


### 2. Criação de Serviços
Desde que o módulo dispatcher é o serviço responsável pelo controle e escalonamento dos demais serviços de coleta, sua execução é realizada diretamente pelo Linux systemd.
Sendo assim, seguem os passos para a criação do serviço "dispatcher": 

copie dispatcher.service para  /etc/systemd/system:  
	# sudo cp dispatcher.service  /etc/systemd/system  

realize a recarga da nova configuração:  
 	# sudo systemctl  daemon-reload  

inicie o  novo serviço:  
	# sudo systemctl start dispatcher 

verifique seu status:  
	# sudo systemctl status dospatcher  

habilite o serviço para ser inicializado durante o reboot:  
	# sudo systemctl enable dispatcher  


### 3. Arquivo de Configuração
O arquivo de configuração dos agentes (coletores), visam definir a localização dos arquivos e nível de severidade dos log de cada agente e os intervalos de execução.
O arquivo segue o padrão yaml e para que qualquer alteração realizada tenha efeito, será necessário reinicializar o serviço dispatcher (sudo systemctl restart dispatcher) 


### 4. Observações
Cada dispatcher está associado a um ou mais partners. Depedendo da carga imposta pelo número de organizações, será necessário que mais VMs ((ou conteiners) sejam adicionadas.
