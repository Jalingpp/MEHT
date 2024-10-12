var fromAddress = web3.eth.accounts[0];
var toAddress = web3.eth.accounts[1];
var v = web3.toWei(100, 'ether');
web3.personal.unlockAccount(web3.eth.accounts[0],"123456");

web3.eth.sendTransaction({from:fromAddress,to:toAddress,value:v})