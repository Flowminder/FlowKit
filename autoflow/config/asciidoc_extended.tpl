{% extends 'asciidoc/index.asciidoc.j2' %}

{% block header %}
:imagesdir: {{ resources['output_files_dir'] }}
{% endblock header %}

{% block input %}
{% if resources.global_content_filter.include_input_prompt %}
{%- endif -%}
{%- if cell['cell_type'] == 'markdown' -%}
{{ cell.source}}
{%- endif -%}
{% endblock input %}

{% block output_group %}
{% if resources.global_content_filter.include_output_prompt %}
{%- endif %}
{% block outputs %}
{{- super() -}}
{% endblock outputs %}
{% endblock output_group %}

{% block data_svg %}
image::{{output.metadata.filenames['image/svg+xml'].split('/')[-1]}}[pdfwidth=90%]
{% endblock data_svg %}

{% block data_png %}
image::{{output.metadata.filenames['image/png'].split('/')[-1]}}[pdfwidth=90%]
{% endblock data_png %}

{% block data_jpg %}
image::{{output.metadata.filenames['image/jpeg'].split('/')[-1]}}[pdfwidth=90%]
{% endblock data_jpg %}

{% block stream %}
{% endblock stream %}

{% block data_text scoped %}
----
{{ super() }}
----
{% endblock data_text %}
