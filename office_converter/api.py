#!/usr/bin/env python
#coding: utf-8

import simplejson as json

from twisted.web.resource import Resource

from twisted.web.static import File

from seafevents.office_converter.task_manager import task_manager

__all_ = [
    "root",
    "StaticFile",
]

class Root(Resource):
    """Root URL handling"""
    def render_GET(self, request):
        request.setResponseCode(404)
        return "please visit URL /convert or /status"

class Convert(Resource):
    """Handle /convert URL. There must be a 'url' param in GET/POST request"""
    isLeaf = True
    
    def render_GET(self, request):
        args = request.args
        supported_doctypes = ('pdf', 'doc', 'docx', 'ppt', 'pptx', 'xls', 'xlsx')
        if not (args.has_key('url') and args.has_key('doctype') and args.has_key('file_id')):
            request.setResponseCode(400)
            return "" 
        elif len(args['file_id'][0]) != 40:
            request.setResponseCode(400)
            return "invalid length obj_id" 
        elif args['doctype'][0] not in supported_doctypes:
            request.setResponseCode(400)
            return "invalid doctype" 
        else:
            # OK, valid check done
            file_id = args['file_id'][0]
            url = args['url'][0]
            doctype = args['doctype'][0]
            
            ret = task_manager.add_task(file_id, doctype, url)
            request.setHeader("content-type", 'application/json; charset=utf-8')
            return json.dumps(ret)

    render_POST = render_GET

class Status(Resource):
    """Handle /status URL. There must be a 'uuid' param in GET/POST request"""
    isLeaf = True
    
    def render_GET(self, request):
        args = request.args
        if not args.has_key('file_id'):
            # bad request
            request.setResponseCode(400)
            return ""
        elif len(args['file_id'][0]) != 40:
            request.setResponseCode(400)
            return "invalid file_id length"
        else:
            file_id = args['file_id'][0]
            status = task_manager.query_task_status(file_id)
            # if 'error' in status:
            #     request.setResponseCode(400)
            #     return status['error']

            request.setHeader("content-type", 'application/json; charset=utf-8')
            return "xx(%s)" % json.dumps(status)

    render_POST = render_GET
    
class CrossDomainXML(Resource):
    """Adobe Flash needs to get /crossdomain.xml to access flash on a
    different host.

    """
    isLeaf = True

    content = '''\
<?xml version="1.0"?>
<cross-domain-policy>
  <allow-access-from domain="*" secure="false" />
</cross-domain-policy>
'''
    
    def render_GET(self, request):
        request.setHeader("content-type", 'application/xml; charset=utf-8')
        return self.content

class StaticFile(File):
    """Overwrite default render_GET method of the File class to add a special
    'Access-Control-Allow-Origin' header for pdfjs to fetch the file

    """

    def render_GET(self, request):
        # Must set this for pdfjs to fetch the pdf
        request.setHeader("Access-Control-Allow-Origin", '*')
        return File.render_GET(self, request)

root = Root()
root.putChild('convert', Convert())
root.putChild('status', Status())
root.putChild('crossdomain.xml', CrossDomainXML())

