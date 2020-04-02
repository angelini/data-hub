import pathlib

import jinja2


def main():
    project_root = pathlib.Path(__file__).parent.parent.absolute()

    env = jinja2.Environment(loader=jinja2.FileSystemLoader(project_root.joinpath('config/templates')))
    nginx = env.get_template('nginx.conf.j2')

    with open(project_root.joinpath('nginx.conf'), 'w') as fh:
        fh.write(nginx.render(cwd=project_root))


if __name__ == '__main__':
    main()
