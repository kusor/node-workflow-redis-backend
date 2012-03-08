A backend for [node-workflow](http://kusor.github.com/node-workflow/) built
over [Redis](http://redis.io/).

# Installation

npm install wf-redis-backend

# Usage

Add the following to the config file of your application using wf:

    {
      "backend": {
        "module": "wf-redis-backend",
        "opts": {
          "port": 6379,
          "host": "127.0.0.1",
          "db": 15
        }
      }
    }

Where `port` and `host` are obviously related to your redis server and `db` is
the Redis Database you want to use. If your Redis server is password protected,
add also `"password": "whatever"` to the configuration section above. 

And that should be it. `wf` REST API and Runners should take care of
properly loading the module on init.

# Issues

See [node-workflow issues](https://github.com/kusor/node-workflow/issues).

# LICENSE

The MIT License (MIT) Copyright (c) 2012 Pedro Palazón Candel

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

