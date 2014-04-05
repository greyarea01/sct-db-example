/**
 * Created by hays on 02/04/14.
 */

angular.module('snapshot', [
    'ui.bootstrap',
    'snapshot-crates']).
    config(['$routeProvider', '$locationProvider',
        function($routeProvider,$locationProvider) {
            $routeProvider.
                when('/', {redirectTo: '/crates'});
            $locationProvider.html5Mode(false);
        }]);