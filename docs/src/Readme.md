# tztz - tools for toolz

Various helpers to simplify using [toolz][toolz]. Features include:

- execute toolz functionality on top of dask bag to parallelize data analysis
- more to come ...

[toolz]: http://toolz.readthedocs.io/en/latest/


See [docs/Api.md](docs/Api.md) for a guide on `tztz`.

## Development

Execute tests with:

```bash
python -m pytest tests
```

To update, change the source files in the `docs/src` folder and execute:

```bash
python docs/update_docs.py
```

## License

>  The MIT License (MIT)
>  Copyright (c) 2017 Christopher Prohm
>
>  Permission is hereby granted, free of charge, to any person obtaining a copy
>  of this software and associated documentation files (the "Software"), to
>  deal in the Software without restriction, including without limitation the
>  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
>  sell copies of the Software, and to permit persons to whom the Software is
>  furnished to do so, subject to the following conditions:
>
>  The above copyright notice and this permission notice shall be included in
>  all copies or substantial portions of the Software.
>
>  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
>  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
>  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
>  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
>  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
>  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
>  DEALINGS IN THE SOFTWARE.
