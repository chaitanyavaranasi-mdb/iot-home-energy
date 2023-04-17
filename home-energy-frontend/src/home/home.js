import react from 'react';
import { connect } from 'react-redux';
import { bindActionCreators } from 'redux';
import { getHomeData } from './home.actions';

class Home extends React.Component {
    componentDidMount() {
        this.props.getHomeData();
    }
    
    render() {
        return (
        <div>
            <h1>Home</h1>
            <p>Home Page</p>
        </div>
        <div>
            <div>Map Data</div>
        </div>
        );
    }
}