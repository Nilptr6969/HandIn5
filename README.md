# Setup

To set up the system, you must first set up the nodes you want to have (up to 3 nodes), by running "go run .\node\ [id]" in different terminals, where id is a unique id from 0-2 (both included). 

After the nodes have been set up, set up as many clients as you want with "go run .\client\"

If you set up a new node after the system has started running, the ongoing bid might be wiped. 

# Usage

When starting up a client, you must first enter a name that will be used to identify the client. 

To use the program, you can run one of two operations in any of the clients. The two commands available are:

- "bid [amount]" where amount is an integer
- "result"

To test a crash failure, use 'ctrl' + 'c' in the terminal of a running server to crash it, then continue using the clients. 