# FlowAuth

Documentation for FlowAuth can be found in the main [FlowKit documentation](https://flowkit.xyz).

## Dev install

You need MySQL client installed on your host machine before you can run the FlowAuth frontend locally.

On Mac:

```bash
brew install mysql
```

On Ubuntu:

```bash
sudo apt install mysql-client default-libmysqlclient-dev
```

The startup script needs access to the FlowKit environment variables. The easiest option is to just link them:

```bash
ln -s ../development_environment .env
```

Then run

```
bash start.sh
```