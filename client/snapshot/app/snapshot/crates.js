/**
 * Created by hays on 02/04/14.
 */

angular.module('snapshot-crates',[])
    .config(['$routeProvider',
    function($routeProvider){
        $routeProvider
            .when('/crates', {
                templateUrl: 'app/snapshot/snapshot.html',
              controller: 'CratesCtrl'
           });
    }])
    .controller('CratesCtrl',['$scope','$http','$location',
    function($scope,$http,$location) {

        $scope.crateModel={};
        $scope.rodModel={};
        $scope.murModel={};
        $scope.modModel={};
        $scope.chipModel={};
        $scope.crateModel.child=$scope.rodModel;
        $scope.rodModel.child=$scope.murModel;
        $scope.murModel.child=$scope.modModel;
        $scope.modModel.child=$scope.chipModel;
        $scope.chipModel.child=null;

        $scope.iov='now'

        $scope.buildAPIURL =function(topModel,iov) {
            var url='api/crates/'+iov+'/';
            var model=topModel;
            var finished=false;
            while(!finished) {
                if(model.selected>=0) {
                    url+=model.urlelement+'/';
                    model=model.child;
                    if(model===null) {
                        finished=true;
                    }
                } else {
                    url+='all';
                    finished=true;
                }

            }
            return url;

        };

        $scope.buildURL =function(topModel,iov) {
            var url='/crates/'+iov+'/';
            var model=topModel;
            var finished=false;
            while(!finished) {
                if(model.selected>=0) {
                    url+=model.urlelement+'/';
                    model=model.child;
                    if(model===null) {
                        finished=true;
                    }
                } else {
                    url+='all';
                    finished=true;
                }

            }
            return url;

        };


        var apiurl=$scope.buildAPIURL($scope.crateModel,$scope.iov);
        var url=$scope.buildURL($scope.crateModel,$scope.iov);

        $http.get(apiurl)
            .success(function(data,status,headers,config){
                console.log('CratesCtrl:'+JSON.stringify(data));
                $scope.resetModel($scope.crateModel);
                $scope.crateModel.data=data;
                $location.path=url;
            });

        $scope.isShown=function(element,model) {
            //console.log(element+' '+model.selected);
            if(model.selected>=0) {
                if( element===model.selected) {
                    return true;
                } else {
                    return false;
                }
            }
            return true;
        };

        $scope.modClick=function(box, index, module,moduleID,model){
            console.log(index+' '+module+' '+moduleID)
            if(index==1 || index==2) {
                if(model.selected===module) {
                    if( model.urlelement===moduleID) {
                       model.selected=-1;
                        model.urlelement=-1;
                        model.lastSelected.selected='';
                        model.lastSelected=null;
                        $scope.resetModel(model.child);
                        return;
                    }
                }
                if(model.lastSelected) {
                    model.lastSelected.selected='';
                }

                model.selected=module;
                model.urlelement=moduleID;
                box.selected='bg-info'
                model.lastSelected=box;
                var apiurl=$scope.buildAPIURL($scope.crateModel,$scope.iov);
                var url=$scope.buildURL($scope.crateModel,$scope.iov);
                console.log(apiurl);
                console.log(url);
                $http.get(apiurl).success(function(data,status,headers,config) {
                    $scope.resetModel(model.child);
                    console.log(JSON.stringify(data));
                    model.child.data=data;
                    model.child.selected=-1;
                    model.child.urlelement=-1;
                    model.child.lastSelected=null;
                    $location.path=url;
                });
            }
        }

        $scope.click=function(element,row,model) {
            if(model.child===null) {
                console.log('No children. Just return');
                return;
            }
            if(model.selected===element) {
                console.log('You clicked it again!');
                model.selected=-1;
                model.urlelement=-1;
                model.lastSelected.selected='';
                model.lastSelected=null;
                $scope.resetModel(model.child);
                return;
            }
            if(model.lastSelected) {
                model.lastSelected.selected='';
            }
            model.selected=element;
            model.urlelement=element;
            row.selected='bg-info';
            model.lastSelected=row;
            var apiurl = $scope.buildAPIURL($scope.crateModel,$scope.iov);
            var url=$scope.buildURL($scope.crateModel,$scope.iov);
            console.log(url);
            console.log(apiurl);
            $http.get(apiurl).success(function(data,status,headers,config) {
               $scope.resetModel(model.child);
               model.child.data=data;
               model.child.selected=-1;
               model.child.urlelement=-1;
               model.child.lastSelected=null;
                $location.path=url;
            });

        }

        $scope.resetModel = function(model) {
            if(model===null) {
                return;
            }
            model.data=null;
            model.selected=-1;
            model.urlelement=-1;
            model.lastSelected=null;
            $scope.resetModel(model.child);
        }
    }]);