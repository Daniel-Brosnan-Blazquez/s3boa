"""
Specific instantiation for the S3 visualization tool

Written by DEIMOS Space S.L. (dibb)

module s3vboa
"""
# Import python utilities
import os

# Import flask utilities
from flask import Flask, send_from_directory
from flask_debugtoolbar import DebugToolbarExtension
import jinja2

# Import vboa
import vboa
# from s3vboa.views.dhus_availability import dhus_availability

def create_app():
    """
    Create and configure an instance of vboa application.
    """
    app = vboa.create_app()

    # Register the specific views
    # app.register_blueprint(dhus_availability.bp)
    
    # Register the specific templates folder
    s2vboa_templates_folder = os.path.dirname(__file__) + "/templates"
    templates_loader = jinja2.ChoiceLoader([
        jinja2.FileSystemLoader(s2vboa_templates_folder),
        app.jinja_loader
    ])
    app.jinja_loader = templates_loader

    # Register the specific static folder
    s3vboa_static_folder = os.path.dirname(__file__) + "/static"
    @app.route('/s3_static_images/<path:filename>')
    def s3_static(filename):
        return send_from_directory(s3vboa_static_folder + "/images", filename)
    # end def
    
    return app
